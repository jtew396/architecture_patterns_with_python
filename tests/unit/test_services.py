import pytest
from datetime import date, timedelta
from allocation.adapters import repository
from allocation.domain.model import Batch
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
    
    @staticmethod
    def for_batch(ref, sky, qty, eta=None):
        return FakeRepository([
            Batch(ref, sky, qty, eta),
        ])


class FakeSession:
    committed = False

    def commit(self):
        self.committed = True


def test_returns_allocation():
    repo = FakeRepository.for_batch("batch1", "COMPLICATED-LAMP", 100, eta=None)
    result = services.allocate("o1", "COMPLICATED-LAMP", 10, repo, FakeSession())
    assert result == "batch1"


def test_error_for_invalid_sku():
    repo = FakeRepository.for_batch("b1", "AREALSKU", 100, eta=None)
    with pytest.raises(services.InvalidSku, match="Invalid sku NONEXISTENTSKU"):
        services.allocate("o1", "NONEXISTENTSKU", 10, repo, FakeSession())


def test_error_for_out_of_stock():
    repo = FakeRepository.for_batch("b1", "TALL-CHAIR", 9, eta=None)
    with pytest.raises(services.OutOfStock, match="Out of stock for sku TALL-CHAIR"):
        services.allocate("o1", "TALL-CHAIR", 10, repo, FakeSession())


def test_commits():
    repo = FakeRepository.for_batch("b1", "OMINOUS-MIRROR", 100, eta=None)
    session = FakeSession()
    services.allocate("o1", "OMINOUS-MIRROR", 10, repo, session)
    assert session.committed is True


def test_deallocate_decrements_available_quantity():
    repo, session = FakeRepository([]), FakeSession()
    services.add_batch("b1", "BLUE-PLINTH", 100, None, repo, session)
    services.allocate("o1", "BLUE-PLINTH", 10, repo, session)
    batch = repo.get(reference="b1")
    assert batch.available_quantity == 90
    services.deallocate("o1", "BLUE-PLINTH", 10, repo, session)
    assert batch.available_quantity == 100


def test_deallocate_decrements_correct_quantity():
    repo, session = FakeRepository([]), FakeSession()
    services.add_batch("b1", "DOG-BED-SMALL", 100, None, repo, session)
    services.add_batch("b2", "DOG-BED-LARGE", 100, None, repo, session)
    services.allocate("o1", "DOG-BED-SMALL", 10, repo, session)
    batch1 = repo.get(reference="b1")
    batch2 = repo.get(reference="b2")
    assert batch1.available_quantity == 90
    assert batch2.available_quantity == 100
    services.deallocate("o1", "DOG-BED-SMALL", 10, repo, session)
    assert batch1.available_quantity == 100
    assert batch2.available_quantity == 100


def test_trying_to_deallocate_unallocated_batch():
    repo, session = FakeRepository([]), FakeSession()
    services.add_batch("b1", "POPULAR-CURTAINS", 100, None, repo, session)
    with pytest.raises(services.NotAllocated, match="Line o1 has not been allocated"):
        services.deallocate("o1", "POPULAR-CURTAINS", 10, repo, session)


def test_prefers_current_stock_batches_to_shipments():
    in_stock_batch = Batch("in-stock-batch", "RETRO-CLOCK", 100, eta=None)
    shipment_batch = Batch("shipment-batch", "RETRO-CLOCK", 100, eta=tomorrow)
    repo = FakeRepository([in_stock_batch, shipment_batch])
    session = FakeSession()
    services.allocate("oref", "RETRO-CLOCK", 10, repo, session)
    assert in_stock_batch.available_quantity == 90
    assert shipment_batch.available_quantity == 100


def test_prefers_earlier_batches():
    earliest = Batch("speedy-batch", "MINIMALIST-SPOON", 100, eta=today)
    medium = Batch("normal-batch", "MINIMALIST-SPOON", 100, eta=tomorrow)
    latest = Batch("slow-batch", "MINIMALIST-SPOON", 100, eta=later)
    repo = FakeRepository([earliest, medium, latest])
    session = FakeSession()
    services.allocate("order1", "MINIMALIST-SPOON", 10, repo, session)
    assert earliest.available_quantity == 90
    assert medium.available_quantity == 100
    assert latest.available_quantity == 100


def test_returns_allocated_batch_ref():
    in_stock_batch = Batch("in-stock-batch-ref", "HIGHBROW-POSTER", 100, eta=None)
    shipment_batch = Batch("shipment-batch-ref", "HIGHBROW-POSTER", 100, eta=tomorrow)
    repo = FakeRepository([in_stock_batch, shipment_batch])
    session = FakeSession()
    allocation = services.allocate("oref", "HIGHBROW-POSTER", 10, repo, session)
    assert allocation == in_stock_batch.reference


def test_raises_out_of_stock_exception_if_cannot_allocate():
    repo = FakeRepository.for_batch('batch1', 'SMALL-FORK', 10, eta=today)
    session = FakeSession()
    services.allocate('order1', 'SMALL-FORK', 10, repo, session)
    with pytest.raises(services.OutOfStock, match='SMALL-FORK'):
        services.allocate('order2', 'SMALL-FORK', 1, repo, session)