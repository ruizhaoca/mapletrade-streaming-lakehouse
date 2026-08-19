from __future__ import annotations

from mapletrade.quality import payload_hash, validate_record


def valid_record() -> dict[str, object]:
    return {
        "event_id": "E1",
        "trade_id": "T1",
        "event_version": 1,
        "event_type": "NEW",
        "event_ts": 1_786_454_400_000,
        "produced_ts": 1_786_454_401_000,
        "portfolio_id": "P1",
        "instrument_id": "I1",
        "side": "BUY",
        "quantity": 100,
        "price": "12.5000",
        "trade_currency": "CAD",
    }


def test_valid_record_passes() -> None:
    result = validate_record(valid_record(), {"I1"}, {"P1"}, {"CAD"})
    assert result.valid
    assert result.failed_rules == ()


def test_invalid_record_returns_all_relevant_rules() -> None:
    record = valid_record()
    record.update(quantity=-1, instrument_id="UNKNOWN", trade_currency="JPY")
    result = validate_record(record, {"I1"}, {"P1"}, {"CAD"})
    assert not result.valid
    assert set(result.failed_rules) == {
        "INVALID_QUANTITY",
        "UNKNOWN_INSTRUMENT",
        "MISSING_FX_RATE",
    }


def test_payload_hash_ignores_delivery_identifiers() -> None:
    first = valid_record()
    second = {**first, "event_id": "E2", "produced_ts": 1_900_000_000_000}
    assert payload_hash(first) == payload_hash(second)
