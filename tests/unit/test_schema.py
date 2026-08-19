from __future__ import annotations

import json
from pathlib import Path


def test_avro_schema_has_required_contract_fields() -> None:
    schema_path = Path(__file__).resolve().parents[2] / "schemas" / "trade_event_v1.avsc"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    field_names = {field["name"] for field in schema["fields"]}
    assert {
        "event_id",
        "trade_id",
        "event_version",
        "event_type",
        "event_ts",
        "portfolio_id",
        "instrument_id",
        "quantity",
        "price",
    } <= field_names
