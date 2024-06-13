from flask import Flask, request
from datetime import datetime

from allocation.adapters import orm
from allocation.domain import events
from allocation.service_layer import messagebus, unit_of_work
from allocation.service_layer.handlers import InvalidSku, OutOfStock


orm.start_mappers()
app = Flask(__name__)


@app.route("/allocate", methods=["POST"])
def allocate_endpoint():
    try:
        event = events.AllocationRequired(
            request.json["orderid"],
            request.json["sku"],
            request.json["qty"]
        )
        results = messagebus.handle(event, unit_of_work.SqlAlchemyUnitOfWork())
        batchref = results.pop(0)
    except (OutOfStock, InvalidSku) as e:
        return {"message": str(e)}, 400
    return {"batchref": batchref}, 201


@app.route("/add_batch", methods=["POST"])
def add_batch():
    eta = request.json["eta"]
    if eta is not None:
        eta = datetime.fromisoformat(eta).date()
    try:
        event = events.BatchCreated(
            request.json["ref"],
            request.json["sku"],
            request.json["qty"],
            eta
        )
        results = messagebus.handle(event, unit_of_work.SqlAlchemyUnitOfWork())
        batchref = results.pop(0)
    except InvalidSku as e:
        return {"message": str(e)}, 400
    return {"batchref": batchref}, 201


@app.route("/deallocate", methods=["POST"])
def deallocate():
    try:
        event = events.DeallocationRequired(
            request.json["orderid"],
            request.json["sku"],
            request.json["qty"]
        )
        results = messagebus.handle(event, unit_of_work.SqlAlchemyUnitOfWork())
        batchref = results.pop(0)
    except InvalidSku as e:
        return {"message": str(e)}, 400
    return {"batchref": batchref}, 201