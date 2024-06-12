from flask import Flask, request
from datetime import datetime

from allocation.adapters import orm
from allocation.service_layer import services, unit_of_work


orm.start_mappers()
app = Flask(__name__)


@app.route("/allocate", methods=["POST"])
def allocate_endpoint():
    uow = unit_of_work.SqlAlchemyUnitOfWork()
    try:
        batchref = services.allocate(
            request.json["orderid"],
            request.json["sku"],
            request.json["qty"],
            uow
        )
    except (services.OutOfStock, services.InvalidSku) as e:
        return {"message": str(e)}, 400
    return {"batchref": batchref}, 201


@app.route("/add_batch", methods=["POST"])
def add_batch():
    uow = unit_of_work.SqlAlchemyUnitOfWork()
    eta = request.json["eta"]
    if eta is not None:
        eta = datetime.fromisoformat(eta).date()
    services.add_batch(
        request.json["ref"],
        request.json["sku"],
        request.json["qty"],
        eta,
        uow
    )
    return "OK", 201


@app.route("/deallocate", methods=["POST"])
def deallocate():
    uow = unit_of_work.SqlAlchemyUnitOfWork()
    try:
        services.deallocate(
            request.json["orderid"],
            request.json["sku"],
            request.json["qty"],
            uow
        )
    except services.InvalidSku as e:
        return {"message": str(e)}, 400
    return "OK", 201