import pytest
from datetime import date, timedelta
from allocation.adapters import repository
from allocation.service_layer import services, unit_of_work

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


def test_returns_allocation():
    uow = FakeUnitOfWork()
    services.add_batch("batch1", "COMPLICATED-LAMP", 100, None, uow)
    result = services.allocate("o1", "COMPLICATED-LAMP", 10, uow)
    assert result == "batch1"


def test_error_for_invalid_sku():
    uow = FakeUnitOfWork()
    services.add_batch("b1", "AREALSKU", 100, None, uow)
    with pytest.raises(services.InvalidSku, match="Invalid sku NONEXISTENTSKU"):
        services.allocate("o1", "NONEXISTENTSKU", 10, uow)


def test_error_for_out_of_stock():
    uow = FakeUnitOfWork()
    services.add_batch("b1", "TALL-CHAIR", 9, None, uow)
    with pytest.raises(services.OutOfStock, match="Out of stock for sku TALL-CHAIR"):
        services.allocate("o1", "TALL-CHAIR", 10, uow)


def test_commits():
    uow = FakeUnitOfWork()
    services.add_batch("b1", "OMINOUS-MIRROR", 100, None, uow)
    services.allocate("o1", "OMINOUS-MIRROR", 10, uow)
    assert uow.committed is True


def test_deallocate_decrements_available_quantity():
    uow = FakeUnitOfWork()
    services.add_batch("b1", "BLUE-PLINTH", 100, None, uow)
    services.allocate("o1", "BLUE-PLINTH", 10, uow)
    batch = uow.products.get("BLUE-PLINTH").get_batch(reference="b1")
    assert batch.available_quantity == 90
    services.deallocate("o1", "BLUE-PLINTH", 10, uow)
    assert batch.available_quantity == 100


def test_deallocate_decrements_correct_quantity():
    uow = FakeUnitOfWork()
    services.add_batch("b1", "DOG-BED-SMALL", 100, None, uow)
    services.add_batch("b2", "DOG-BED-LARGE", 100, None, uow)
    services.allocate("o1", "DOG-BED-SMALL", 10, uow)
    batch1 = uow.products.get("DOG-BED-SMALL").get_batch(reference="b1")
    batch2 = uow.products.get("DOG-BED-LARGE").get_batch(reference="b2")
    assert batch1.available_quantity == 90
    assert batch2.available_quantity == 100
    services.deallocate("o1", "DOG-BED-SMALL", 10, uow)
    assert batch1.available_quantity == 100
    assert batch2.available_quantity == 100


def test_trying_to_deallocate_unallocated_batch():
    uow = FakeUnitOfWork()
    services.add_batch("b1", "POPULAR-CURTAINS", 100, None, uow)
    with pytest.raises(services.NotAllocated, match="Line o1 has not been allocated"):
        services.deallocate("o1", "POPULAR-CURTAINS", 10, uow)


def test_prefers_current_stock_batches_to_shipments():
    uow = FakeUnitOfWork()
    services.add_batch("in-stock-batch", "RETRO-CLOCK", 100, None, uow)
    services.add_batch("shipment-batch", "RETRO-CLOCK", 100, tomorrow, uow)
    services.allocate("oref", "RETRO-CLOCK", 10, uow)
    assert uow.products.get("RETRO-CLOCK").get_batch("in-stock-batch").available_quantity == 90
    assert uow.products.get("RETRO-CLOCK").get_batch("shipment-batch").available_quantity == 100


def test_prefers_earlier_batches():
    uow = FakeUnitOfWork()
    services.add_batch("speedy-batch", "MINIMALIST-SPOON", 100, today, uow)
    services.add_batch("normal-batch", "MINIMALIST-SPOON", 100, tomorrow, uow)
    services.add_batch("slow-batch", "MINIMALIST-SPOON", 100, later, uow)
    services.allocate("order1", "MINIMALIST-SPOON", 10, uow)
    assert uow.products.get("MINIMALIST-SPOON").get_batch("speedy-batch").available_quantity == 90
    assert uow.products.get("MINIMALIST-SPOON").get_batch("normal-batch").available_quantity == 100
    assert uow.products.get("MINIMALIST-SPOON").get_batch("slow-batch").available_quantity == 100


def test_returns_allocated_batch_ref():
    uow = FakeUnitOfWork()
    services.add_batch("in-stock-batch-ref", "HIGHBROW-POSTER", 100, None, uow)
    services.add_batch("shipment-batch-ref", "HIGHBROW-POSTER", 100, tomorrow, uow)
    allocation = services.allocate("oref", "HIGHBROW-POSTER", 10, uow)
    assert allocation == uow.products.get("HIGHBROW-POSTER").get_batch("in-stock-batch-ref").reference


def test_raises_out_of_stock_exception_if_cannot_allocate():
    uow = FakeUnitOfWork()
    services.add_batch('batch1', 'SMALL-FORK', 10, today, uow)
    services.allocate('order1', 'SMALL-FORK', 10, uow)
    with pytest.raises(services.OutOfStock, match='SMALL-FORK'):
        services.allocate('order2', 'SMALL-FORK', 1, uow)


def test_add_batch_for_new_product():
    uow = FakeUnitOfWork()
    services.add_batch("b1", "CRUNCHY-ARMCHAIR", 100, None, uow)
    assert uow.products.get("CRUNCHY-ARMCHAIR").get_batch("b1") is not None
    assert uow.committed


def test_add_batch_for_existing_product():
    uow = FakeUnitOfWork()
    services.add_batch("b1", "GARISH-RUG", 100, None, uow)
    services.add_batch("b2", "GARISH-RUG", 99, None, uow)
    assert "b2" in [b.reference for b in uow.products.get("GARISH-RUG").batches]


def test_allocate_returns_allocation():
    uow = FakeUnitOfWork()
    services.add_batch("batch1", "COMPLICATED-LAMP", 100, None, uow)
    result = services.allocate("o1", "COMPLICATED-LAMP", 10, uow)
    assert result == "batch1"


def test_allocate_errors_for_invalid_sku():
    uow = FakeUnitOfWork()
    services.add_batch("b1", "AREALSKU", 100, None, uow)
    with pytest.raises(services.InvalidSku, match="Invalid sku NONEXISTENTSKU"):
        services.allocate("o1", "NONEXISTENTSKU", 10, uow)