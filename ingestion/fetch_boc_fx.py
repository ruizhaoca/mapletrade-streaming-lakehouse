from __future__ import annotations

import argparse
import csv
import json
from collections.abc import Sequence
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

SERIES_TO_CURRENCY = {
    "FXUSDCAD": "USD",
    "FXEURCAD": "EUR",
    "FXGBPCAD": "GBP",
}
BASE_URL = "https://www.bankofcanada.ca/valet/observations"
UTC = timezone.utc


def build_url(start_date: date, end_date: date) -> str:
    series = ",".join(SERIES_TO_CURRENCY)
    query = urlencode({"start_date": start_date.isoformat(), "end_date": end_date.isoformat()})
    return f"{BASE_URL}/{series}/json?{query}"


def download_json(url: str, timeout: int = 30) -> bytes:
    with urlopen(url, timeout=timeout) as response:  # noqa: S310 - fixed HTTPS source
        return response.read()


def normalize_observations(
    payload: dict[str, Any], requested_start: date, requested_end: date, retrieved_at: str
) -> list[dict[str, str]]:
    observed: dict[str, dict[date, Decimal]] = {
        currency: {} for currency in SERIES_TO_CURRENCY.values()
    }
    for observation in payload.get("observations", []):
        observed_date = date.fromisoformat(observation["d"])
        for series, currency in SERIES_TO_CURRENCY.items():
            value = observation.get(series, {}).get("v")
            if value not in (None, ""):
                observed[currency][observed_date] = Decimal(value)

    rows: list[dict[str, str]] = []
    current = requested_start
    while current <= requested_end:
        rows.append(
            {
                "currency": "CAD",
                "rate_date": current.isoformat(),
                "cad_rate": "1.000000",
                "original_rate_date": current.isoformat(),
                "is_carried_forward": "false",
                "source": "BANK_OF_CANADA_IDENTITY_RATE",
                "retrieved_at": retrieved_at,
            }
        )
        for currency, values in observed.items():
            available_dates = [value_date for value_date in values if value_date <= current]
            if not available_dates:
                continue
            original_date = max(available_dates)
            rows.append(
                {
                    "currency": currency,
                    "rate_date": current.isoformat(),
                    "cad_rate": f"{values[original_date]:.6f}",
                    "original_rate_date": original_date.isoformat(),
                    "is_carried_forward": str(original_date != current).lower(),
                    "source": "BANK_OF_CANADA_VALET_API",
                    "retrieved_at": retrieved_at,
                }
            )
        current += timedelta(days=1)
    return rows


def write_csv(rows: list[dict[str, str]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--raw-output", type=Path, default=Path("data/raw/boc/fx_response.json"))
    parser.add_argument(
        "--normalized-output",
        type=Path,
        default=Path("data/processed/reference/fx_rates.csv"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.end_date < args.start_date:
        raise ValueError("end-date cannot be earlier than start-date")

    retrieval_start = args.start_date - timedelta(days=7)
    url = build_url(retrieval_start, args.end_date)
    raw = download_json(url)
    args.raw_output.parent.mkdir(parents=True, exist_ok=True)
    args.raw_output.write_bytes(raw)

    retrieved_at = datetime.now(UTC).isoformat()
    rows = normalize_observations(
        json.loads(raw), args.start_date, args.end_date, retrieved_at=retrieved_at
    )
    if not rows:
        raise RuntimeError("Bank of Canada response contained no usable observations")
    write_csv(rows, args.normalized_output)
    print(f"Downloaded: {url}")
    print(f"Raw response: {args.raw_output}")
    print(f"Normalized rows: {len(rows):,} -> {args.normalized_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
