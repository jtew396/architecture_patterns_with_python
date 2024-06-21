import pytest
from collections import defaultdict
from datetime import date, timedelta
from typing import Dict, List
from allocation import bootstrap
from allocation.adapters import notifications, repository
from allocation.domain import commands
from allocation.service_layer import handlers, unit_of_work


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

    def _get_by_batchref(self, batchref):
        return next((p for p in self._products for b in p.batches if b.reference == batchref), None)


class FakeUnitOfWork(unit_of_work.AbstractUnitOfWork):
    def __init__(self):
        self.products = FakeRepository([])
        self.committed = False

    def _commit(self):
        self.committed = True

    def rollback(self):
        pass


class FakeNotifications(notifications.AbstractNotifications):
    def __init__(self):
        self.sent = defaultdict(list)   # type: Dict[str, List[str]]

    def send(self, destination, message):
        self.sent[destination].append(message)


def bootstrap_test_app():
    return bootstrap.bootstrap(
        start_orm=False,
        uow=FakeUnitOfWork(),
        notifications=FakeNotifications(),
        publish=lambda *args: None,
    )


class TestAddBatch:
    def test_for_new_product(self):
        bus = bootstrap_test_app()
        bus.handle(commands.CreateBatch("b1", "CRUNCHY-ARMCHAIR", 100, None))
        assert bus.uow.products.get("CRUNCHY-ARMCHAIR").get_batch("b1") is not None
        assert bus.uow.committed

    def test_for_existing_product(self):
        bus = bootstrap_test_app()
        bus.handle(commands.CreateBatch("b1", "GARISH-RUG", 100, None))
        bus.handle(commands.CreateBatch("b2", "GARISH-RUG", 99, None))
        assert "b2" in [b.reference for b in bus.uow.products.get("GARISH-RUG").batches]

    def test_prefers_earlier_batches(self):
        bus = bootstrap_test_app()
        bus.handle(commands.CreateBatch("speedy-batch", "MINIMALIST-SPOON", 100, today))
        bus.handle(commands.CreateBatch("normal-batch", "MINIMALIST-SPOON", 100, tomorrow))
        bus.handle(commands.CreateBatch("slow-batch", "MINIMALIST-SPOON", 100, later))
        bus.handle(commands.Allocate("order1", "MINIMALIST-SPOON", 10))
        assert bus.uow.products.get("MINIMALIST-SPOON").get_batch("speedy-batch").available_quantity == 90
        assert bus.uow.products.get("MINIMALIST-SPOON").get_batch("normal-batch").available_quantity == 100
        assert bus.uow.products.get("MINIMALIST-SPOON").get_batch("slow-batch").available_quantity == 100
    
    def test_prefers_current_stock_batches_to_shipments(self):
        bus = bootstrap_test_app()
        bus.handle(commands.CreateBatch("in-stock-batch", "RETRO-CLOCK", 100, None))
        bus.handle(commands.CreateBatch("shipment-batch", "RETRO-CLOCK", 100, tomorrow))
        bus.handle(commands.Allocate("oref", "RETRO-CLOCK", 10))
        assert bus.uow.products.get("RETRO-CLOCK").get_batch("in-stock-batch").available_quantity == 90
        assert bus.uow.products.get("RETRO-CLOCK").get_batch("shipment-batch").available_quantity == 100


class TestAllocate:
    def test_allocates(self):
        bus = bootstrap_test_app()
        bus.handle(commands.CreateBatch("batch1", "COMPLICATED-LAMP", 100, None))
        bus.handle(commands.Allocate("o1", "COMPLICATED-LAMP", 10))
        [batch] = bus.uow.products.get("COMPLICATED-LAMP").batches
        assert batch.available_quantity == 90

    def test_errors_for_invalid_sku(self):
        bus = bootstrap_test_app()
        bus.handle(commands.CreateBatch("b1", "AREALSKU", 100, None))
        with pytest.raises(handlers.InvalidSku, match="Invalid sku NONEXISTENTSKU"):
            bus.handle(commands.Allocate("o1", "NONEXISTENTSKU", 10))

    def test_commits(self):
        bus = bootstrap_test_app()
        bus.handle(commands.CreateBatch("b1", "OMINOUS-MIRROR", 100, None))
        bus.handle(commands.Allocate("o1", "OMINOUS-MIRROR", 10))
        assert bus.uow.committed is True

    def test_sends_email_on_out_of_stock_error(self):
        fake_notifs = FakeNotifications()
        bus = bootstrap.bootstrap(
            start_orm=False,
            uow=FakeUnitOfWork(),
            notifications=fake_notifs,
            publish=lambda *args: None,
        )
        bus.handle(commands.CreateBatch("b1", "POPULAR-CURTAINS", 9, None))
        bus.handle(commands.Allocate("o1", "POPULAR-CURTAINS", 10))
        assert fake_notifs.sent["stock@made.com"] == [
            f"Out of stock for POPULAR-CURTAINS",
        ]


class TestDeallocate:
    def decrements_available_quantity(self):
        bus = bootstrap_test_app()
        bus.handle(commands.CreateBatch("b1", "BLUE-PLINTH", 100, None))
        bus.handle(commands.Allocate("o1", "BLUE-PLINTH", 10))
        batch = bus.uow.products.get("BLUE-PLINTH").get_batch(reference="b1")
        assert batch.available_quantity == 90
        bus.handle(commands.Deallocate("o1", "BLUE-PLINTH", 10))
        assert batch.available_quantity == 100

    def decrements_correct_quantity(self):
        bus = bootstrap_test_app()
        bus.handle(commands.CreateBatch("b1", "DOG-BED-SMALL", 100, None))
        bus.handle(commands.CreateBatch("b2", "DOG-BED-LARGE", 100, None))
        bus.handle(commands.Allocate("o1", "DOG-BED-SMALL", 10))
        batch1 = bus.uow.products.get("DOG-BED-SMALL").get_batch(reference="b1")
        batch2 = bus.uow.products.get("DOG-BED-LARGE").get_batch(reference="b2")
        assert batch1.available_quantity == 90
        assert batch2.available_quantity == 100
        bus.handle(commands.Deallocate("o1", "DOG-BED-SMALL", 10))
        assert batch1.available_quantity == 100
        assert batch2.available_quantity == 100

    def test_errors_for_unallocated_batch(self):
        bus = bootstrap_test_app()
        bus.handle(commands.CreateBatch("b1", "POPULAR-CURTAINS", 100, None))
        with pytest.raises(handlers.NotAllocated, match="Line o1 has not been allocated"):
            bus.handle(commands.Deallocate("o1", "POPULAR-CURTAINS", 10))


class TestChangeBatchQuantity:
    def test_changes_available_quantity(self):
        bus = bootstrap_test_app()
        bus.handle(commands.CreateBatch("batch1", "ADORABLE-SETTEE", 100, None))
        [batch] = bus.uow.products.get("ADORABLE-SETTEE").batches
        assert batch.available_quantity == 100
        bus.handle(commands.ChangeBatchQuantity("batch1", 50))
        assert batch.available_quantity == 50

    def test_reallocates_if_necessary(self):
        bus = bootstrap_test_app()
        event_history = [
            commands.CreateBatch("batch1", "INDIFFERENT-TABLE", 50, None),
            commands.CreateBatch("batch2", "INDIFFERENT-TABLE", 50, date.today()),
            commands.Allocate("order1", "INDIFFERENT-TABLE", 20),
            commands.Allocate("order2", "INDIFFERENT-TABLE", 20),
        ]
        for e in event_history:
            bus.handle(e)
        [batch1, batch2] = bus.uow.products.get("INDIFFERENT-TABLE").batches
        assert batch1.available_quantity == 10
        assert batch2.available_quantity == 50
        bus.handle(commands.ChangeBatchQuantity("batch1", 25))
        assert batch1.available_quantity == 5
        assert batch2.available_quantity == 30