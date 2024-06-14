from dataclasses import dataclass


class Event:
    pass


@dataclass
class OutOfStock(Event):
    sku: str


@dataclass
class NotAllocated(Event):
    orderid: str