import json
import logging
import redis
from allocation import config
from allocation.adapters import orm
from allocation.domain import commands
from allocation.service_layer import messagebus, unit_of_work


logger = logging.getLogger(__name__)


r = redis.Redis(**config.get_redis_host_and_port())


def main():
    orm.start_mappers()
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe("allocate")
    pubsub.subscribe("add_batch")
    pubsub.subscribe("change_batch_quantity")
    pubsub.subscribe("deallocate")

    for m in pubsub.listen():
        logger.debug("handling message %s", m)
        if m["channel"] == b"allocate":
            handle_allocate(m)
        elif m["channel"] == b"add_batch":
            handle_add_batch(m)
        elif m["channel"] == b"change_batch_quantity":
            handle_change_batch_quantity(m)
        elif m["channel"] == b"deallocate":
            handle_deallocate(m)
        else:
            logger.warning("unknown message %s", m)


def handle_allocate(m):
    logger.debug("handling %s", m)
    data = json.loads(m["data"])
    cmd = commands.Allocate(orderid=data["orderid"], sku=data["sku"], qty=data["qty"])
    messagebus.handle(cmd, uow=unit_of_work.SqlAlchemyUnitOfWork())


def handle_add_batch(m):
    logger.debug("handling %s", m)
    data = json.loads(m["data"])
    cmd = commands.CreateBatch(
        ref=data["ref"], sku=data["sku"], qty=data["qty"], eta=data["eta"]
    )
    messagebus.handle(cmd, uow=unit_of_work.SqlAlchemyUnitOfWork())


def handle_change_batch_quantity(m):
    logger.debug("handling %s", m)
    data = json.loads(m["data"])
    cmd = commands.ChangeBatchQuantity(ref=data["batchref"], qty=data["qty"])
    messagebus.handle(cmd, uow=unit_of_work.SqlAlchemyUnitOfWork())


def handle_deallocate(m):
    logger.debug("handling %s", m)
    data = json.loads(m["data"])
    cmd = commands.Deallocate(orderid=data["orderid"], sku=data["sku"], qty=data["qty"])
    messagebus.handle(cmd, uow=unit_of_work.SqlAlchemyUnitOfWork())


if __name__ == "__main__":
    main()