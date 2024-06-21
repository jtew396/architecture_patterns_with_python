from flask import Flask, jsonify, request
from datetime import datetime

from allocation import bootstrap, views
from allocation.domain import commands
from allocation.service_layer.handlers import InvalidSku, OutOfStock


app = Flask(__name__)
bus = bootstrap.bootstrap()


@app.route("/allocations/<orderid>", methods=["GET"])
def allocations_view_endpoint(orderid):
    result = views.allocations(orderid, bus.uow)
    if not result:
        return "not found", 404
    return jsonify(result), 200


@app.route("/allocate", methods=["POST"])
def allocate_endpoint():
    try:
        cmd = commands.Allocate(
            request.json["orderid"],
            request.json["sku"],
            request.json["qty"]
        )
        bus.handle(cmd)
    except (InvalidSku) as e:
        return {"message": str(e)}, 400
    return "OK", 202


@app.route("/add_batch", methods=["POST"])
def add_batch():
    eta = request.json["eta"]
    if eta is not None:
        eta = datetime.fromisoformat(eta).date()
    try:
        cmd = commands.CreateBatch(
            request.json["ref"],
            request.json["sku"],
            request.json["qty"],
            eta
        )
        results = bus.handle(cmd)
        batchref = results.pop(0)
    except InvalidSku as e:
        return {"message": str(e)}, 400
    return {"batchref": batchref}, 201


@app.route("/deallocate", methods=["POST"])
def deallocate():
    try:
        cmd = commands.Deallocate(
            request.json["orderid"],
            request.json["sku"],
            request.json["qty"]
        )
        results = bus.handle(cmd)
        batchref = results.pop(0)
    except InvalidSku as e:
        return {"message": str(e)}, 400
    return {"batchref": batchref}, 201