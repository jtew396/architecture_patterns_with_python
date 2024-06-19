import json
import pytest
from tenacity import Retrying, stop_after_delay
from . import api_client, redis_client
from ..random_refs import random_batchref, random_orderid, random_sku


@pytest.mark.usefixtures("postgres_db")
@pytest.mark.usefixtures("restart_api")
@pytest.mark.usefixtures("restart_redis_pubsub")
def test_allocate_leading_to_line_allocated():
    batchref, sku, qty, eta = random_batchref(), random_sku(), 100, None
    api_client.post_to_add_batch(batchref, sku, qty, eta)

    orderid = random_orderid()

    subscription = redis_client.subscribe_to("line_allocated")

    redis_client.publish_message(
        "allocate",
        {"orderid": orderid, "sku": sku, "qty": 10},
    )

    messages = []
    for attempt in Retrying(stop=stop_after_delay(3), reraise=True):
        with attempt:
            message = subscription.get_message(timeout=1)
            if message:
                messages.append(message)
                print(messages)
            data = json.loads(messages[-1]["data"])
            assert data["orderid"] == orderid
            assert data["batchref"] == batchref


@pytest.mark.usefixtures("postgres_db")
@pytest.mark.usefixtures("restart_api")
@pytest.mark.usefixtures("restart_redis_pubsub")
def test_add_batch_leading_to_batch_created():
    ref, sku, qty, eta = random_batchref(), random_sku(), 100, None

    subscription = redis_client.subscribe_to("batch_created")

    redis_client.publish_message(
        "add_batch",
        {"ref": ref, "sku": sku, "qty": qty, "eta": eta},
    )

    messages = []
    for attempt in Retrying(stop=stop_after_delay(3), reraise=True):
        with attempt:
            message = subscription.get_message(timeout=1)
            if message:
                messages.append(message)
                print(messages)
            data = json.loads(messages[-1]["data"])
            assert data["ref"] == ref
            assert data["sku"] == sku
            assert data["qty"] == qty
            assert data["eta"] == eta


@pytest.mark.usefixtures("postgres_db")
@pytest.mark.usefixtures("restart_api")
@pytest.mark.usefixtures("restart_redis_pubsub")
def test_change_batch_quantity_leading_to_reallocation():
    # start with two batches and an order allocated to one of them
    orderid, sku = random_orderid(), random_sku()
    earlier_batch, later_batch = random_batchref("old"), random_batchref("newer")
    api_client.post_to_add_batch(earlier_batch, sku, qty=10, eta="2011-01-01")
    api_client.post_to_add_batch(later_batch, sku, qty=10, eta="2011-01-02")
    response = api_client.post_to_allocate(orderid, sku, 10)
    assert response.status_code == 202

    response = api_client.get_allocation(orderid)
    assert response.ok
    assert response.json() == [{"sku": sku, "batchref": earlier_batch}]

    subscription = redis_client.subscribe_to("line_allocated")

    # change quantity on allocated batch so it's less than our order
    redis_client.publish_message(
        "change_batch_quantity",
        {"batchref": earlier_batch, "qty": 5},
    )

    # wait until we see a message saying the order has been reallocated
    messages = []
    for attempt in Retrying(stop=stop_after_delay(3), reraise=True):
        with attempt:
            message = subscription.get_message(timeout=1)
            if message:
                messages.append(message)
                print(messages)
            data = json.loads(messages[-1]["data"])
            assert data["orderid"] == orderid
            assert data["batchref"] == later_batch


@pytest.mark.usefixtures("postgres_db")
@pytest.mark.usefixtures("restart_api")
@pytest.mark.usefixtures("restart_redis_pubsub")
def test_deallocate_leading_to_line_deallocated():
    batchref, sku, qty, eta = random_batchref(), random_sku(), 100, None
    api_client.post_to_add_batch(batchref, sku, qty, eta)

    orderid = random_orderid()
    response = api_client.post_to_allocate(orderid, sku, qty=10)
    assert response.status_code == 202

    response = api_client.get_allocation(orderid)
    assert response.ok
    assert response.json() == [{"sku": sku, "batchref": batchref}]

    subscription = redis_client.subscribe_to("line_deallocated")

    redis_client.publish_message(
        "deallocate",
        {"orderid": orderid, "sku": sku, "qty": 10},
    )

    messages = []
    for attempt in Retrying(stop=stop_after_delay(3), reraise=True):
        with attempt:
            message = subscription.get_message(timeout=1)
            if message:
                messages.append(message)
                print(messages)
            data = json.loads(messages[-1]["data"])
            assert data["orderid"] == orderid
            assert data["batchref"] == batchref