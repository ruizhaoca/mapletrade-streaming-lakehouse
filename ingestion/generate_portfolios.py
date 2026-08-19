from __future__ import annotations

import argparse
import csv
from collections.abc import Sequence
from pathlib import Path

PORTFOLIOS = (
    ("PORT-001", "Canadian Growth Fund", "LONG_ONLY", "CAD"),
    ("PORT-002", "North America Equity Fund", "LONG_SHORT", "CAD"),
    ("PORT-003", "Dividend Income Fund", "LONG_ONLY", "CAD"),
    ("PORT-004", "Technology Opportunities Fund", "LONG_SHORT", "CAD"),
    ("PORT-005", "Internal Trading Book", "TRADING", "CAD"),
)


def write_portfolios(output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "portfolio_id",
                "portfolio_name",
                "strategy",
                "base_currency",
                "active_flag",
                "effective_date",
                "source",
            ]
        )
        for portfolio_id, name, strategy, base_currency in PORTFOLIOS:
            writer.writerow(
                [portfolio_id, name, strategy, base_currency, "true", "2026-01-01", "SYNTHETIC"]
            )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, default=Path("data/processed/reference/portfolios.csv")
    )
    args = parser.parse_args(argv)
    write_portfolios(args.output)
    print(f"Wrote {len(PORTFOLIOS)} synthetic portfolios to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
