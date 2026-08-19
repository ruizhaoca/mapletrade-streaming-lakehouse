from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any


class EventType(str, Enum):
    NEW = "NEW"
    AMEND = "AMEND"
    CANCEL = "CANCEL"


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class TradeStatus(str, Enum):
    BOOKED = "BOOKED"
    CANCELLED = "CANCELLED"


class Outcome(str, Enum):
    APPLIED = "APPLIED"
    DUPLICATE = "DUPLICATE"
    QUARANTINED = "QUARANTINED"
    STALE_VERSION = "STALE_VERSION"
    VERSION_CONFLICT = "VERSION_CONFLICT"


@dataclass(frozen=True, slots=True)
class TradeEvent:
    event_id: str
    trade_id: str
    event_version: int
    event_type: EventType
    event_ts: int
    produced_ts: int
    source_system: str
    portfolio_id: str
    instrument_id: str
    side: Side
    quantity: int
    price: str
    trade_currency: str

    def to_dict(self) -> dict[str, Any]:
        record = asdict(self)
        record["event_type"] = self.event_type.value
        record["side"] = self.side.value
        return record

    @classmethod
    def from_dict(cls, record: dict[str, Any]) -> TradeEvent:
        return cls(
            event_id=str(record["event_id"]),
            trade_id=str(record["trade_id"]),
            event_version=int(record["event_version"]),
            event_type=EventType(record["event_type"]),
            event_ts=int(record["event_ts"]),
            produced_ts=int(record["produced_ts"]),
            source_system=str(record.get("source_system", "SYNTHETIC_OMS")),
            portfolio_id=str(record["portfolio_id"]),
            instrument_id=str(record["instrument_id"]),
            side=Side(record["side"]),
            quantity=int(record["quantity"]),
            price=str(record["price"]),
            trade_currency=str(record["trade_currency"]),
        )
