from __future__ import annotations

import csv
from datetime import date

from ingestion.fetch_boc_fx import normalize_observations
from ingestion.prepare_tmx_instruments import normalize_tmx_csv


def test_boc_rates_are_carried_forward() -> None:
    payload = {
        "observations": [
            {
                "d": "2026-08-07",
                "FXUSDCAD": {"v": "1.3943"},
                "FXEURCAD": {"v": "1.6121"},
                "FXGBPCAD": {"v": "1.8816"},
            }
        ]
    }
    rows = normalize_observations(
        payload,
        date(2026, 8, 8),
        date(2026, 8, 9),
        retrieved_at="2026-08-10T00:00:00Z",
    )
    usd_rows = [row for row in rows if row["currency"] == "USD"]
    assert len(usd_rows) == 2
    assert all(row["is_carried_forward"] == "true" for row in usd_rows)
    assert all(row["original_rate_date"] == "2026-08-07" for row in usd_rows)


def test_tmx_normalization_marks_synthetic_fields(tmp_path) -> None:
    source = tmp_path / "tmx.csv"
    with source.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["Company Name", "Symbol", "Exchange"])
        writer.writerow(["Example Issuer", "ABC", "TSX"])

    rows = normalize_tmx_csv(source, limit=25)
    assert rows[0]["symbol"] == "ABC"
    assert rows[0]["instrument_id"].startswith("INS-")
    assert rows[0]["quote_currency_is_synthetic"] == "true"
