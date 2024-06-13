import pytest
from datetime import date, timedelta
from allocation.adapters import repository
from allocation.service_layer import handlers, messagebus, unit_of_work
from allocation.domain import events

today = date.today()
tomorrow = today + timedelta(days=1)
later = tomorrow + timedelta(days=10)


class FakeRepository(repository.AbstractProductRepository):
    def __init__(self, products):
        super().__init__()
        self._products = set(products)

    def _add(self, product):
        self._products.add(product)

    def _get(self, sku):
        return next((p for p in self._products if p.sku == sku), None)


class FakeUnitOfWork(unit_of_work.AbstractUnitOfWork):
    def __init__(self):
        self.products = FakeRepository([])
        self.committed = False

    def _commit(self):
        self.committed = True

    def rollback(self):
        pass


class TestAddBatch:
    def test_for_new_product(self):
        uow = FakeUnitOfWork()
        messagebus.handle(events.BatchCreated("b1", "CRUNCHY-ARMCHAIR", 100, None), uow)
        assert uow.products.get("CRUNCHY-ARMCHAIR").get_batch("b1") is not None
        assert uow.committed

    def test_for_existing_product(self):
        uow = FakeUnitOfWork()
        messagebus.handle(events.BatchCreated("b1", "GARISH-RUG", 100, None), uow)
        messagebus.handle(events.BatchCreated("b2", "GARISH-RUG", 99, None), uow)
        assert "b2" in [b.reference for b in uow.products.get("GARISH-RUG").batches]

    def test_prefers_earlier_batches(self):
        uow = FakeUnitOfWork()
        messagebus.handle(events.BatchCreated("speedy-batch", "MINIMALIST-SPOON", 100, today), uow)
        messagebus.handle(events.BatchCreated("normal-batch", "MINIMALIST-SPOON", 100, tomorrow), uow)
        messagebus.handle(events.BatchCreated("slow-batch", "MINIMALIST-SPOON", 100, later), uow)
        messagebus.handle(events.AllocationRequired("order1", "MINIMALIST-SPOON", 10), uow)
        assert uow.products.get("MINIMALIST-SPOON").get_batch("speedy-batch").available_quantity == 90
        assert uow.products.get("MINIMALIST-SPOON").get_batch("normal-batch").available_quantity == 100
        assert uow.products.get("MINIMALIST-SPOON").get_batch("slow-batch").available_quantity == 100
    
    def test_prefers_current_stock_batches_to_shipments(self):
        uow = FakeUnitOfWork()
        messagebus.handle(events.BatchCreated("in-stock-batch", "RETRO-CLOCK", 100, None), uow)
        messagebus.handle(events.BatchCreated("shipment-batch", "RETRO-CLOCK", 100, tomorrow), uow)
        messagebus.handle(events.AllocationRequired("oref", "RETRO-CLOCK", 10), uow)
        assert uow.products.get("RETRO-CLOCK").get_batch("in-stock-batch").available_quantity == 90
        assert uow.products.get("RETRO-CLOCK").get_batch("shipment-batch").available_quantity == 100


class TestAllocate:
    def test_returns_allocation(self):
        uow = FakeUnitOfWork()
        messagebus.handle(events.BatchCreated("batch1", "COMPLICATED-LAMP", 100, None), uow)
        results = messagebus.handle(events.AllocationRequired("o1", "COMPLICATED-LAMP", 10), uow)
        assert results.pop(0) == "batch1"

    def test_returns_allocated_batch_ref(self):
        uow = FakeUnitOfWork()
        messagebus.handle(events.BatchCreated("in-stock-batch-ref", "HIGHBROW-POSTER", 100, None), uow)
        messagebus.handle(events.BatchCreated("shipment-batch-ref", "HIGHBROW-POSTER", 100, tomorrow), uow)
        results = messagebus.handle(events.AllocationRequired("oref", "HIGHBROW-POSTER", 10), uow)
        assert results.pop(0) == uow.products.get("HIGHBROW-POSTER").get_batch("in-stock-batch-ref").reference



class TestError:
    def for_invalid_sku(self):
        uow = FakeUnitOfWork()
        messagebus.handle(events.BatchCreated("b1", "AREALSKU", 100, None), uow)
        with pytest.raises(handlers.InvalidSku, match="Invalid sku NONEXISTENTSKU"):
            messagebus.handle(events.AllocationRequired("o1", "NONEXISTENTSKU", 10), uow)

    def for_out_of_stock(self):
        uow = FakeUnitOfWork()
        messagebus.handle(events.BatchCreated("b1", "TALL-CHAIR", 9, None), uow)
        with pytest.raises(handlers.OutOfStock, match="Out of stock for sku TALL-CHAIR"):
            messagebus.handle(events.AllocationRequired("o1", "TALL-CHAIR", 10), uow)
    
    def for_out_of_stock_exception_if_cannot_allocate(self):
        uow = FakeUnitOfWork()
        messagebus.handle(events.BatchCreated("batch1", "SMALL-FORK", 10, None), uow)
        messagebus.handle(events.AllocationRequired("order1", "SMALL-FORK", 10), uow)
        with pytest.raises(handlers.OutOfStock, match='SMALL-FORK'):
            messagebus.handle(events.AllocationRequired("order2", "SMALL-FORK", 1), uow)


class TestCommit:
    def test_commits(self):
        uow = FakeUnitOfWork()
        messagebus.handle(events.BatchCreated("b1", "OMINOUS-MIRROR", 100, None), uow)
        messagebus.handle(events.AllocationRequired("o1", "OMINOUS-MIRROR", 10), uow)
        assert uow.committed is True


class TestDeallocate:
    def decrements_available_quantity(self):
        uow = FakeUnitOfWork()
        messagebus.handle(events.BatchCreated("b1", "BLUE-PLINTH", 100, None), uow)
        messagebus.handle(events.AllocationRequired("o1", "BLUE-PLINTH", 10), uow)
        batch = uow.products.get("BLUE-PLINTH").get_batch(reference="b1")
        assert batch.available_quantity == 90
        messagebus.handle(events.DeallocationRequired("o1", "BLUE-PLINTH", 10), uow)
        assert batch.available_quantity == 100

    def decrements_correct_quantity(self):
        uow = FakeUnitOfWork()
        messagebus.handle(events.BatchCreated("b1", "DOG-BED-SMALL", 100, None), uow)
        messagebus.handle(events.BatchCreated("b2", "DOG-BED-LARGE", 100, None), uow)
        messagebus.handle(events.AllocationRequired("o1", "DOG-BED-SMALL", 10), uow)
        batch1 = uow.products.get("DOG-BED-SMALL").get_batch(reference="b1")
        batch2 = uow.products.get("DOG-BED-LARGE").get_batch(reference="b2")
        assert batch1.available_quantity == 90
        assert batch2.available_quantity == 100
        messagebus.handle(events.DeallocationRequired("o1", "DOG-BED-SMALL", 10), uow)
        assert batch1.available_quantity == 100
        assert batch2.available_quantity == 100

    def for_unallocated_batch(self):
        uow = FakeUnitOfWork()
        messagebus.handle(events.BatchCreated("b1", "POPULAR-CURTAINS", 100, None), uow)
        with pytest.raises(handlers.NotAllocated, match="Line o1 has not been allocated"):
            messagebus.handle(events.DeallocationRequired("o1", "POPULAR-CURTAINS", 10), uow)