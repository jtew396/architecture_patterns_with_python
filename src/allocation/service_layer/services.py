from allocation.adapters.repository import AbstractRepository
from allocation.domain import model
from allocation.domain.model import OrderLine


class InvalidSku(Exception):
    pass


class OutOfStock(Exception):
    pass


class NotAllocated(Exception):
    pass


def is_valid_sku(sku, batches):
    return sku in {b.sku for b in batches}


def is_in_stock(line, batches):
    return sum(b.available_quantity for b in batches if b.sku == line.sku) >= line.qty


def is_allocated(line, batches):
    return line in {line for batch in batches for line in batch._allocations}


def allocate(line: OrderLine, repo: AbstractRepository, session) -> str:
    batches = repo.list()
    if not is_valid_sku(line.sku, batches):
        raise InvalidSku(f"Invalid sku {line.sku}")
    if not is_in_stock(line, batches):
        raise OutOfStock(f"Out of stock for sku {line.sku}")
    batchref = model.allocate(line, batches)
    session.commit()
    return batchref


def add_batch(ref, sku, qty, eta, repo, session):
    repo.add(model.Batch(ref, sku, qty, eta))
    session.commit()


def deallocate(line: OrderLine, repo: AbstractRepository, session) -> str:
    batches = repo.list()
    if not is_valid_sku(line.sku, batches):
        raise InvalidSku(f"Invalid sku {line.sku}")
    if not is_allocated(line, batches):
        raise NotAllocated(f"Line {line.orderid} has not been allocated")
    model.deallocate(line, batches)
    session.commit()