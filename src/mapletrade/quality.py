from __future__ import annotations

import hashlib
import json
from collections.abc import Collection
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

PAYLOAD_FIELDS = (
    "trade_id",
    "event_version",
    "event_type",
    "event_ts",
    "portfolio_id",
    "instrument_id",
    "side",
    "quantity",
    "price",
    "trade_currency",
)


@dataclass(frozen=True, slots=True)
class QualityResult:
    valid: bool
    failed_rules: tuple[str, ...]


def payload_hash(record: dict[str, Any]) -> str:
    """Hash business content while excluding delivery-specific identifiers and timestamps."""
    canonical = {field: record.get(field) for field in PAYLOAD_FIELDS}
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def validate_record(
    record: dict[str, Any],
    known_instruments: Collection[str],
    active_portfolios: Collection[str],
    supported_currencies: Collection[str],
) -> QualityResult:
    failures: list[str] = []
    if not record.get("event_id"):
        failures.append("MISSING_EVENT_ID")
    if not record.get("trade_id"):
        failures.append("MISSING_TRADE_ID")
    if not isinstance(record.get("event_version"), int) or record["event_version"] <= 0:
        failures.append("INVALID_EVENT_VERSION")
    if record.get("event_type") not in {"NEW", "AMEND", "CANCEL"}:
        failures.append("INVALID_EVENT_TYPE")
    if not record.get("event_ts"):
        failures.append("MISSING_EVENT_TIMESTAMP")
    if record.get("side") not in {"BUY", "SELL"}:
        failures.append("INVALID_SIDE")
    if not isinstance(record.get("quantity"), int) or record["quantity"] <= 0:
        failures.append("INVALID_QUANTITY")
    try:
        if Decimal(str(record.get("price"))) <= 0:
            failures.append("INVALID_PRICE")
    except (InvalidOperation, TypeError):
        failures.append("INVALID_PRICE")
    if record.get("instrument_id") not in known_instruments:
        failures.append("UNKNOWN_INSTRUMENT")
    if record.get("portfolio_id") not in active_portfolios:
        failures.append("UNKNOWN_OR_INACTIVE_PORTFOLIO")
    if record.get("trade_currency") not in supported_currencies:
        failures.append("MISSING_FX_RATE")
    return QualityResult(valid=not failures, failed_rules=tuple(failures))
