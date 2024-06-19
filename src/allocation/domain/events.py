from dataclasses import dataclass


class Event:
    pass


@dataclass
class OutOfStock(Event):
    sku: str


@dataclass
class Allocated(Event):
    orderid: str
    sku: str
    qty: int
    batchref: str


@dataclass
class BatchCreated(Event):
    ref: str
    sku: str
    qty: int
    eta: str


@dataclass
class Deallocated(Event):
    orderid: str
    sku: str
    qty: int
    batchref: str


@dataclass
class NotAllocated(Event):
    orderid: str