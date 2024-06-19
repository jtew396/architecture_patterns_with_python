from sqlalchemy import text
from allocation.adapters import email, redis_eventpublisher
from allocation.domain import events, model, commands
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


def allocate(command: commands.Allocate, uow: unit_of_work.AbstractUnitOfWork) -> str:
    line = model.OrderLine(command.orderid, command.sku, command.qty)
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


def add_batch(command: commands.CreateBatch, uow: unit_of_work.AbstractUnitOfWork):
    with uow:
        product = uow.products.get(sku=command.sku)
        if product is None:
            product = model.Product(command.sku, batches=[])
            uow.products.add(product)
        product.add_batch(model.Batch(command.ref, command.sku, command.qty, command.eta))
        uow.commit()


def deallocate(command: commands.Deallocate, uow: unit_of_work.AbstractUnitOfWork) -> str:
    line = model.OrderLine(command.orderid, command.sku, command.qty)
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


def change_batch_quantity(command: commands.ChangeBatchQuantity, uow: unit_of_work.AbstractUnitOfWork):
    with uow:
        product = uow.products.get_by_batchref(batchref=command.ref)
        product.change_batch_quantity(ref=command.ref, qty=command.qty)
        uow.commit()


def send_out_of_stock_notification(event: events.OutOfStock, uow: unit_of_work.AbstractUnitOfWork):
    email.send(
        "stock@made.com",
        f"Out of stock for {event.sku}",
    )


def publish_allocated_event(event: events.Allocated, uow: unit_of_work.AbstractUnitOfWork):
    redis_eventpublisher.publish("line_allocated", event)


def publish_batch_created_event(event: events.BatchCreated, uow: unit_of_work.AbstractUnitOfWork):
    redis_eventpublisher.publish("batch_created", event)


def publish_deallocated_event(event: events.Deallocated, uow: unit_of_work.AbstractUnitOfWork):
    redis_eventpublisher.publish("line_deallocated", event)


def add_allocation_to_read_model(event: events.Allocated, uow: unit_of_work.AbstractUnitOfWork):
    with uow:
        uow.session.execute(
            text(
                """
                INSERT INTO allocations_view (orderid, sku, batchref)
                VALUES (:orderid, :sku, :batchref)
                """
            ),
            dict(orderid=event.orderid, sku=event.sku, batchref=event.batchref)
        )
        uow.commit()


def remove_allocation_from_read_model(event: events.Deallocated, uow: unit_of_work.AbstractUnitOfWork):
    with uow:
        uow.session.execute(
            text(
                """
                DELETE FROM allocations_view
                WHERE orderid = :orderid AND sku = :sku
                """
            ),
            dict(orderid=event.orderid, sku=event.sku)
        )
        uow.commit()