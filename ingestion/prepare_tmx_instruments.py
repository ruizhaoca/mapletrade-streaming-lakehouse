from __future__ import annotations

import argparse
import csv
import hashlib
from collections.abc import Iterable, Sequence
from datetime import datetime, timezone
from pathlib import Path

SOURCE_URL = "https://www.tsx.com/en/listings/listing-with-us/listed-company-directory"
UTC = timezone.utc


def find_column(fieldnames: Iterable[str], candidates: tuple[str, ...]) -> str:
    normalized = {name.strip().lower(): name for name in fieldnames}
    for candidate in candidates:
        if candidate in normalized:
            return normalized[candidate]
    raise ValueError(f"Could not find any of {candidates!r} in columns {list(fieldnames)!r}")


def stable_instrument_id(exchange: str, symbol: str) -> str:
    digest = hashlib.sha256(f"{exchange}:{symbol}".encode()).hexdigest()[:8].upper()
    return f"INS-{digest}"


def normalize_tmx_csv(input_path: Path, limit: int) -> list[dict[str, str]]:
    with input_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("TMX input has no header row")
        symbol_column = find_column(reader.fieldnames, ("symbol", "ticker", "stock symbol"))
        name_column = find_column(
            reader.fieldnames, ("company name", "issuer name", "company", "name")
        )
        try:
            exchange_column = find_column(reader.fieldnames, ("exchange", "market"))
        except ValueError:
            exchange_column = ""

        normalized: list[dict[str, str]] = []
        for index, source_row in enumerate(reader):
            symbol = source_row.get(symbol_column, "").strip().upper()
            issuer = source_row.get(name_column, "").strip()
            exchange = (
                source_row.get(exchange_column, "TSX").strip().upper() if exchange_column else "TSX"
            )
            if not symbol or not issuer:
                continue
            # Currency is explicitly project-generated enrichment, not a TMX-sourced field.
            currency_cycle = ("CAD", "CAD", "CAD", "USD", "CAD", "EUR", "CAD", "GBP")
            normalized.append(
                {
                    "instrument_id": stable_instrument_id(exchange, symbol),
                    "symbol": symbol,
                    "issuer_name": issuer,
                    "exchange": exchange,
                    "asset_class": "EQUITY",
                    "quote_currency": currency_cycle[index % len(currency_cycle)],
                    "active_flag": "true",
                    "effective_date": "2026-01-01",
                    "source": "TMX_ISSUER_LIST",
                    "source_url": SOURCE_URL,
                    "retrieved_at": datetime.now(UTC).isoformat(),
                    "quote_currency_is_synthetic": "true",
                }
            )
            if len(normalized) >= limit:
                break
    if not normalized:
        raise ValueError("TMX input produced no usable instrument rows")
    return sorted(normalized, key=lambda row: (row["exchange"], row["symbol"]))


def write_csv(rows: list[dict[str, str]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Normalize a manually downloaded TMX issuer CSV for MapleTrade."
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument(
        "--output", type=Path, default=Path("data/processed/reference/instruments.csv")
    )
    parser.add_argument("--limit", type=int, default=25)
    args = parser.parse_args(argv)
    rows = normalize_tmx_csv(args.input, args.limit)
    write_csv(rows, args.output)
    print(f"Normalized {len(rows)} TMX instruments -> {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
