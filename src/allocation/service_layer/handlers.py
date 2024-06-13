from allocation.adapters import email
from allocation.domain import events, model
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


def allocate(event: events.AllocationRequired, uow: unit_of_work.AbstractUnitOfWork) -> str:
    line = model.OrderLine(event.orderid, event.sku, event.qty)
    with uow:
        product = uow.products.get(sku=line.sku)
        if product is None:
            raise InvalidSku(f"Invalid sku {line.sku}")
        if not is_valid_sku(line.sku, product.batches):
            raise InvalidSku(f"Invalid sku {line.sku}")
        if not is_in_stock(line, product.batches):
            raise OutOfStock(f"Out of stock for sku {line.sku}")
        batchref = product.allocate(line)
        uow.commit()
    return batchref


def add_batch(event: events.BatchCreated, uow: unit_of_work.AbstractUnitOfWork):
    with uow:
        product = uow.products.get(sku=event.sku)
        if product is None:
            product = model.Product(event.sku, batches=[])
            uow.products.add(product)
        product.batches.append(model.Batch(event.ref, event.sku, event.qty, event.eta))
        uow.commit()


def deallocate(event: events.DeallocationRequired, uow: unit_of_work.AbstractUnitOfWork) -> str:
    line = model.OrderLine(event.orderid, event.sku, event.qty)
    with uow:
        product = uow.products.get(sku=line.sku)
        if product is None:
            raise InvalidSku(f"Invalid sku {line.sku}")
        if not is_valid_sku(line.sku, product.batches):
            raise InvalidSku(f"Invalid sku {line.sku}")
        if not is_allocated(line, product.batches):
            raise NotAllocated(f"Line {line.orderid} has not been allocated")
        product.deallocate(line)
        uow.commit()


def change_batch_quantity(event: events.BatchQuantityChanged, uow: unit_of_work.AbstractUnitOfWork):
    with uow:
        product = uow.products.get_by_batchref(batchref=event.ref)
        product.change_batch_quantity(ref=event.ref, qty=event.qty)
        uow.commit()


def send_out_of_stock_notification(event: events.OutOfStock, uow: unit_of_work.AbstractUnitOfWork):
    email.send(
        "stock@made.com",
        f"Out of stock for {event.sku}",
    )