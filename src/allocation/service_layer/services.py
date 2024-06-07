from datetime import date
from typing import Optional
from allocation.domain import model
from allocation.service_layer import unit_of_work


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


def allocate(orderid: str, sku: str, qty: int, uow: unit_of_work.AbstractUnitOfWork) -> str:
    line = model.OrderLine(orderid, sku, qty)
    with uow:
        batches = uow.batches.list()
        if not is_valid_sku(line.sku, batches):
            raise InvalidSku(f"Invalid sku {line.sku}")
        if not is_in_stock(line, batches):
            raise OutOfStock(f"Out of stock for sku {line.sku}")
        batchref = model.allocate(line, batches)
        uow.commit()
    return batchref


def add_batch(ref: str, sku: str, qty: int, eta: Optional[date], uow: unit_of_work.AbstractUnitOfWork) -> None:
    with uow:
        uow.batches.add(model.Batch(ref, sku, qty, eta))
        uow.commit()


def deallocate(orderid: str, sku: str, qty: int, uow: unit_of_work.AbstractUnitOfWork) -> str:
    line = model.OrderLine(orderid, sku, qty)
    with uow:
        batches = uow.batches.list()
        if not is_valid_sku(line.sku, batches):
            raise InvalidSku(f"Invalid sku {line.sku}")
        if not is_allocated(line, batches):
            raise NotAllocated(f"Line {line.orderid} has not been allocated")
        model.deallocate(line, batches)
        uow.commit()