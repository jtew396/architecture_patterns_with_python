import pytest
from . import api_client
from ..random_refs import random_sku, random_batchref, random_orderid


@pytest.mark.usefixtures("postgres_db")
@pytest.mark.usefixtures("restart_api")
def test_happy_path_returns_202_and_allocated_batch():
    orderid = random_orderid()
    sku, othersku = random_sku(), random_sku("other")
    earlybatch = random_batchref(1)
    laterbatch = random_batchref(2)
    otherbatch = random_batchref(3)
    api_client.post_to_add_batch(laterbatch, sku, 100, "2011-01-02")
    api_client.post_to_add_batch(earlybatch, sku, 100, "2011-01-01")
    api_client.post_to_add_batch(otherbatch, othersku, 100, None)

    r = api_client.post_to_allocate(orderid, sku, qty=3)
    assert r.status_code == 202

    r = api_client.get_allocation(orderid)
    assert r.ok
    assert r.json() == [{"sku": sku, "batchref": earlybatch}]


@pytest.mark.usefixtures("postgres_db")
@pytest.mark.usefixtures("restart_api")
def test_unhappy_path_returns_400_and_error_message():
    unknown_sku, orderid = random_sku(), random_orderid()
    r = api_client.post_to_allocate(
        orderid, unknown_sku, qty=20, expect_success=False
    )
    assert r.status_code == 400
    assert r.json()["message"] == f"Invalid sku {unknown_sku}"

    r = api_client.get_allocation(orderid)
    assert r.status_code == 404


@pytest.mark.usefixtures("postgres_db")
@pytest.mark.usefixtures("restart_api")
def test_deallocate():
    sku, order1, order2 = random_sku(), random_orderid(), random_orderid()
    batch = random_batchref()
    api_client.post_to_add_batch(batch, sku, 100, "2011-01-02")

    # fully allocate
    r = api_client.post_to_allocate(order1, sku, qty=100)
    assert r.status_code == 202

    r = api_client.get_allocation(order1)
    assert r.ok
    assert r.json() == [{"sku": sku, "batchref": batch}]

    # attempt to allocate second order
    r = api_client.post_to_allocate(order2, sku, qty=100, expect_success=False)
    assert r.ok

    # second order is not in allocations
    r = api_client.get_allocation(order2)
    assert r.status_code == 404

    # deallocate
    r = api_client.post_to_deallocate(order1, sku, qty=100)
    assert r.ok

    # now we can allocate second order
    r = api_client.post_to_allocate(order2, sku, qty=100)
    assert r.ok

    r = api_client.get_allocation(order2)
    assert r.ok
    assert r.json() == [{"sku": sku, "batchref": batch}]