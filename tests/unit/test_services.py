import pytest
from datetime import date, timedelta
from allocation.adapters import repository
from allocation.domain.model import Batch, OrderLine
from allocation.service_layer import services

today = date.today()
tomorrow = today + timedelta(days=1)
later = tomorrow + timedelta(days=10)

class FakeRepository(repository.AbstractRepository):
    def __init__(self, batches):
        self._batches = set(batches)

    def add(self, batch):
        self._batches.add(batch)

    def get(self, reference):
        return next(b for b in self._batches if b.reference == reference)

    def list(self):
        return list(self._batches)
    

class FakeSession:
    committed = False

    def commit(self):
        self.committed = True


def test_returns_allocation():
    line = OrderLine("o1", "COMPLICATED-LAMP", 10)
    batch = Batch("b1", "COMPLICATED-LAMP", 100, eta=None)
    repo = FakeRepository([batch])

    result = services.allocate(line, repo, FakeSession())
    assert result == "b1"


def test_error_for_invalid_sku():
    line = OrderLine("o1", "NONEXISTENTSKU", 10)
    batch = Batch("b1", "AREALSKU", 100, eta=None)
    repo = FakeRepository([batch])

    with pytest.raises(services.InvalidSku, match="Invalid sku NONEXISTENTSKU"):
        services.allocate(line, repo, FakeSession())


def test_error_for_out_of_stock():
    line = OrderLine("o1", "TALL-CHAIR", 10)
    batch = Batch("b1", "TALL-CHAIR", 9, eta=None)
    repo = FakeRepository([batch])

    with pytest.raises(services.OutOfStock, match="Out of stock for sku TALL-CHAIR"):
        services.allocate(line, repo, FakeSession())


def test_commits():
    line = OrderLine("o1", "OMINOUS-MIRROR", 10)
    batch = Batch("b1", "OMINOUS-MIRROR", 100, eta=None)
    repo = FakeRepository([batch])
    session = FakeSession()

    services.allocate(line, repo, session)
    assert session.committed is True


def test_deallocate_decrements_available_quantity():
    repo, session = FakeRepository([]), FakeSession()
    services.add_batch("b1", "BLUE-PLINTH", 100, None, repo, session)
    line = OrderLine("o1", "BLUE-PLINTH", 10)
    services.allocate(line, repo, session)
    batch = repo.get(reference="b1")
    assert batch.available_quantity == 90
    services.deallocate(line, repo, session)
    assert batch.available_quantity == 100


def test_deallocate_decrements_correct_quantity():
    repo, session = FakeRepository([]), FakeSession()
    services.add_batch("b1", "DOG-BED-SMALL", 100, None, repo, session)
    services.add_batch("b2", "DOG-BED-LARGE", 100, None, repo, session)
    line = OrderLine("o1", "DOG-BED-SMALL", 10)
    services.allocate(line, repo, session)
    batch1 = repo.get(reference="b1")
    batch2 = repo.get(reference="b2")
    assert batch1.available_quantity == 90
    assert batch2.available_quantity == 100
    services.deallocate(line, repo, session)
    assert batch1.available_quantity == 100
    assert batch2.available_quantity == 100


def test_trying_to_deallocate_unallocated_batch():
    repo, session = FakeRepository([]), FakeSession()
    services.add_batch("b1", "POPULAR-CURTAINS", 100, None, repo, session)
    with pytest.raises(services.NotAllocated, match="Line o1 has not been allocated"):
        services.deallocate(OrderLine("o1", "POPULAR-CURTAINS", 10), repo, session)


def test_prefers_current_stock_batches_to_shipments():
    in_stock_batch = Batch("in-stock-batch", "RETRO-CLOCK", 100, eta=None)
    shipment_batch = Batch("shipment-batch", "RETRO-CLOCK", 100, eta=tomorrow)
    repo = FakeRepository([in_stock_batch, shipment_batch])
    session = FakeSession()

    line = OrderLine("oref", "RETRO-CLOCK", 10)

    services.allocate(line, repo, session)

    assert in_stock_batch.available_quantity == 90
    assert shipment_batch.available_quantity == 100


def test_prefers_earlier_batches():
    earliest = Batch("speedy-batch", "MINIMALIST-SPOON", 100, eta=today)
    medium = Batch("normal-batch", "MINIMALIST-SPOON", 100, eta=tomorrow)
    latest = Batch("slow-batch", "MINIMALIST-SPOON", 100, eta=later)
    repo = FakeRepository([earliest, medium, latest])
    session = FakeSession()

    line = OrderLine("order1", "MINIMALIST-SPOON", 10)

    services.allocate(line, repo, session)

    assert earliest.available_quantity == 90
    assert medium.available_quantity == 100
    assert latest.available_quantity == 100


def test_returns_allocated_batch_ref():
    in_stock_batch = Batch("in-stock-batch-ref", "HIGHBROW-POSTER", 100, eta=None)
    shipment_batch = Batch("shipment-batch-ref", "HIGHBROW-POSTER", 100, eta=tomorrow)
    repo = FakeRepository([in_stock_batch, shipment_batch])
    session = FakeSession()

    line = OrderLine("oref", "HIGHBROW-POSTER", 10)

    allocation = services.allocate(line, repo, session)
    assert allocation == in_stock_batch.reference


def test_raises_out_of_stock_exception_if_cannot_allocate():
    batch = Batch('batch1', 'SMALL-FORK', 10, eta=today)
    repo = FakeRepository([batch])
    session = FakeSession()

    services.allocate(OrderLine('order1', 'SMALL-FORK', 10), repo, session)

    with pytest.raises(services.OutOfStock, match='SMALL-FORK'):
        services.allocate(OrderLine('order2', 'SMALL-FORK', 1), repo, session)