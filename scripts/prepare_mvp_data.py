from __future__ import annotations

import argparse
import json
import shutil
import sys
from collections.abc import Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
UTC = timezone.utc
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from ingestion.fetch_boc_fx import (  # noqa: E402
    build_url,
    download_json,
    normalize_observations,
    write_csv,
)
from ingestion.prepare_tmx_instruments import normalize_tmx_csv  # noqa: E402
from ingestion.prepare_tmx_instruments import write_csv as write_instruments  # noqa: E402
from mapletrade.generator import (  # noqa: E402
    EventGenerator,
    GenerationConfig,
    load_column,
    load_instrument_currencies,
    write_generation,
)


def prepare_reference_files(args) -> tuple[Path, Path, Path]:
    processed = ROOT / "data" / "processed" / "reference"
    processed.mkdir(parents=True, exist_ok=True)
    instruments = processed / "instruments.csv"
    portfolios = processed / "portfolios.csv"
    fx_rates = processed / "fx_rates.csv"

    if args.tmx_input:
        write_instruments(normalize_tmx_csv(args.tmx_input, args.instrument_limit), instruments)
    else:
        shutil.copy2(ROOT / "data" / "sample" / "reference" / "instruments.csv", instruments)
    shutil.copy2(ROOT / "data" / "sample" / "reference" / "portfolios.csv", portfolios)

    if args.use_sample_fx:
        shutil.copy2(ROOT / "data" / "sample" / "reference" / "fx_rates.csv", fx_rates)
    else:
        retrieval_start = args.event_date - timedelta(days=7)
        url = build_url(retrieval_start, args.event_date)
        raw = download_json(url)
        raw_path = ROOT / "data" / "raw" / "boc" / "fx_response.json"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_bytes(raw)
        rows = normalize_observations(
            json.loads(raw),
            args.event_date,
            args.event_date,
            retrieved_at=datetime.now(UTC).isoformat(),
        )
        write_csv(rows, fx_rates)
    return instruments, portfolios, fx_rates


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare reference CSVs, deterministic events, and a Databricks upload bundle."
    )
    parser.add_argument("--num-trades", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--event-date", type=date.fromisoformat, default=date(2026, 8, 11))
    parser.add_argument("--tmx-input", type=Path)
    parser.add_argument("--instrument-limit", type=int, default=25)
    parser.add_argument(
        "--use-sample-fx",
        action="store_true",
        help="Avoid the network and use the committed 2026-08-11 sample rates.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.use_sample_fx and args.event_date != date(2026, 8, 11):
        raise ValueError("The committed sample FX file only supports event-date 2026-08-11")

    instruments, portfolios, fx_rates = prepare_reference_files(args)
    instrument_currencies = load_instrument_currencies(instruments)
    generator = EventGenerator(
        GenerationConfig(
            num_trades=args.num_trades,
            seed=args.seed,
            event_date=args.event_date.isoformat(),
        ),
        instrument_ids=list(instrument_currencies),
        portfolio_ids=load_column(portfolios, "portfolio_id"),
        instrument_currencies=instrument_currencies,
    )
    events, metadata = generator.generate()
    event_path, metadata_path = write_generation(
        events, metadata, ROOT / "data" / "generated" / "mvp"
    )

    upload_reference = ROOT / "data" / "upload" / "reference"
    upload_replay = ROOT / "data" / "upload" / "replay"
    upload_reference.mkdir(parents=True, exist_ok=True)
    upload_replay.mkdir(parents=True, exist_ok=True)
    for source in (instruments, portfolios, fx_rates):
        shutil.copy2(source, upload_reference / source.name)
    shutil.copy2(event_path, upload_replay / event_path.name)
    shutil.copy2(metadata_path, upload_replay / metadata_path.name)

    print(f"Prepared {len(events):,} messages from {args.num_trades:,} base trades")
    print(f"Reference upload directory: {upload_reference}")
    print(f"Replay upload directory: {upload_replay}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
