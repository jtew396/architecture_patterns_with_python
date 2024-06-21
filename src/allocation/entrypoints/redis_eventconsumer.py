import json
import logging
import redis
from allocation import bootstrap, config
from allocation.domain import commands


logger = logging.getLogger(__name__)


r = redis.Redis(**config.get_redis_host_and_port())


def main():
    logger.info("Redis pubusb starting")
    bus = bootstrap.bootstrap()
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe("allocate")
    pubsub.subscribe("add_batch")
    pubsub.subscribe("change_batch_quantity")
    pubsub.subscribe("deallocate")

    for m in pubsub.listen():
        logger.info("handling message %s", m)
        if m["channel"] == b"allocate":
            handle_allocate(m, bus)
        elif m["channel"] == b"add_batch":
            handle_add_batch(m, bus)
        elif m["channel"] == b"change_batch_quantity":
            handle_change_batch_quantity(m, bus)
        elif m["channel"] == b"deallocate":
            handle_deallocate(m, bus)
        else:
            logger.warning("unknown message %s", m)


def handle_allocate(m, bus):
    logger.info("handling %s", m)
    data = json.loads(m["data"])
    cmd = commands.Allocate(orderid=data["orderid"], sku=data["sku"], qty=data["qty"])
    bus.handle(cmd)


def handle_add_batch(m, bus):
    logger.info("handling %s", m)
    data = json.loads(m["data"])
    cmd = commands.CreateBatch(
        ref=data["ref"], sku=data["sku"], qty=data["qty"], eta=data["eta"]
    )
    bus.handle(cmd)


def handle_change_batch_quantity(m, bus):
    logger.info("handling %s", m)
    data = json.loads(m["data"])
    cmd = commands.ChangeBatchQuantity(ref=data["batchref"], qty=data["qty"])
    bus.handle(cmd)


def handle_deallocate(m, bus):
    logger.info("handling %s", m)
    data = json.loads(m["data"])
    cmd = commands.Deallocate(orderid=data["orderid"], sku=data["sku"], qty=data["qty"])
    bus.handle(cmd)


if __name__ == "__main__":
    main()