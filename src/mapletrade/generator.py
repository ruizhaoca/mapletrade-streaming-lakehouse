from __future__ import annotations

import argparse
import csv
import json
import random
import uuid
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from mapletrade.models import EventType, Side, TradeEvent

UTC = timezone.utc


@dataclass(frozen=True, slots=True)
class GenerationConfig:
    num_trades: int = 10_000
    amend_rate: float = 0.10
    multiple_amend_rate: float = 0.02
    cancel_rate: float = 0.05
    duplicate_rate: float = 0.01
    invalid_rate: float = 0.005
    unknown_reference_rate: float = 0.005
    late_event_rate: float = 0.01
    conflict_rate: float = 0.001
    seed: int = 2026
    event_date: str = "2026-08-11"

    def validate(self) -> None:
        if self.num_trades <= 0:
            raise ValueError("num_trades must be positive")
        for name, value in asdict(self).items():
            if name.endswith("_rate") and not 0 <= value <= 1:
                raise ValueError(f"{name} must be between 0 and 1")


def utc_millis(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def load_column(path: Path, column: str) -> list[str]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        values = [row[column].strip() for row in csv.DictReader(handle) if row.get(column)]
    if not values:
        raise ValueError(f"No values found in {path} column {column!r}")
    return values


def load_instrument_currencies(path: Path) -> dict[str, str]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))
    mapping = {
        row["instrument_id"].strip(): row.get("quote_currency", "CAD").strip() or "CAD"
        for row in rows
        if row.get("instrument_id")
    }
    if not mapping:
        raise ValueError(f"No instrument records found in {path}")
    return mapping


class EventGenerator:
    """Generate deterministic complete-snapshot lifecycle events."""

    def __init__(
        self,
        config: GenerationConfig,
        instrument_ids: Sequence[str],
        portfolio_ids: Sequence[str],
        currencies: Sequence[str] = ("CAD", "USD", "EUR", "GBP"),
        instrument_currencies: dict[str, str] | None = None,
    ) -> None:
        config.validate()
        if not instrument_ids or not portfolio_ids:
            raise ValueError("instrument_ids and portfolio_ids cannot be empty")
        self.config = config
        self.instrument_ids = tuple(instrument_ids)
        self.portfolio_ids = tuple(portfolio_ids)
        self.currencies = tuple(currencies)
        self.instrument_currencies = instrument_currencies or {}
        self.random = random.Random(config.seed)
        self.late_events_injected = 0
        self.base_time = datetime.combine(
            date.fromisoformat(config.event_date), datetime.min.time(), tzinfo=UTC
        ) + timedelta(hours=14)

    def _uuid(self) -> str:
        return str(uuid.UUID(int=self.random.getrandbits(128), version=4))

    def _base_event(self, index: int) -> TradeEvent:
        event_time = self.base_time + timedelta(milliseconds=index * 10)
        price = Decimal(self.random.randrange(500, 50_000)) / Decimal("100")
        instrument_id = self.random.choice(self.instrument_ids)
        trade_currency = self.instrument_currencies.get(instrument_id)
        if not trade_currency:
            trade_currency = self.random.choices(self.currencies, weights=(70, 20, 6, 4), k=1)[0]
        return TradeEvent(
            event_id=self._uuid(),
            trade_id=f"TRD-{index + 1:08d}",
            event_version=1,
            event_type=EventType.NEW,
            event_ts=utc_millis(event_time),
            produced_ts=utc_millis(event_time + timedelta(seconds=1)),
            source_system="SYNTHETIC_OMS",
            portfolio_id=self.random.choice(self.portfolio_ids),
            instrument_id=instrument_id,
            side=self.random.choice((Side.BUY, Side.SELL)),
            quantity=self.random.randrange(1, 501) * 10,
            price=f"{price:.4f}",
            trade_currency=trade_currency,
        )

    def _next_event(self, prior: TradeEvent, event_type: EventType) -> TradeEvent:
        event_time = datetime.fromtimestamp(prior.event_ts / 1000, tz=UTC) + timedelta(minutes=1)
        quantity = prior.quantity
        price = Decimal(prior.price)
        if event_type is EventType.AMEND:
            quantity = max(10, quantity + self.random.choice((-100, -50, 50, 100)))
            price += Decimal(self.random.choice((-25, -10, 10, 25))) / Decimal("100")
        return replace(
            prior,
            event_id=self._uuid(),
            event_version=prior.event_version + 1,
            event_type=event_type,
            event_ts=utc_millis(event_time),
            produced_ts=utc_millis(event_time + timedelta(seconds=1)),
            quantity=quantity,
            price=f"{max(price, Decimal('0.0100')):.4f}",
        )

    def generate(self) -> tuple[list[TradeEvent], dict[str, object]]:
        events: list[TradeEvent] = []
        for index in range(self.config.num_trades):
            current = self._base_event(index)
            events.append(current)

            if self.random.random() < self.config.amend_rate:
                current = self._next_event(current, EventType.AMEND)
                events.append(current)
                if self.random.random() < self.config.multiple_amend_rate:
                    current = self._next_event(current, EventType.AMEND)
                    events.append(current)

            if self.random.random() < self.config.cancel_rate:
                current = self._next_event(current, EventType.CANCEL)
                events.append(current)

        lifecycle_event_count = len(events)
        self._inject_invalid_values(events)
        self._inject_unknown_references(events)

        duplicate_count = min(
            round(lifecycle_event_count * self.config.duplicate_rate), len(events)
        )
        for event in self.random.sample(events, duplicate_count):
            events.append(event)

        conflict_candidates = [
            event
            for event in events
            if event.event_type is EventType.AMEND
            and event.quantity > 0
            and event.instrument_id in self.instrument_ids
            and event.portfolio_id in self.portfolio_ids
        ]
        conflict_count = min(
            round(lifecycle_event_count * self.config.conflict_rate), len(conflict_candidates)
        )
        for event in self.random.sample(conflict_candidates, conflict_count):
            events.append(
                replace(
                    event,
                    event_id=self._uuid(),
                    quantity=event.quantity + 10,
                )
            )

        publish_order = self._apply_late_delivery(events)
        publish_order = [
            replace(
                event,
                produced_ts=utc_millis(self.base_time + timedelta(seconds=index + 1)),
            )
            for index, event in enumerate(publish_order)
        ]

        metadata: dict[str, object] = {
            "generation_run_id": self._uuid(),
            "seed": self.config.seed,
            "base_trade_count": self.config.num_trades,
            "generated_event_count": len(publish_order),
            "lifecycle_event_count_before_injections": lifecycle_event_count,
            "duplicate_events_injected": duplicate_count,
            "same_version_conflicts_injected": conflict_count,
            "late_events_injected": self.late_events_injected,
            "rates": {
                key: value for key, value in asdict(self.config).items() if key.endswith("_rate")
            },
            "avro_schema_version": 1,
            "event_date": self.config.event_date,
            "generated_at": datetime.now(UTC).isoformat(),
        }
        return publish_order, metadata

    def _inject_invalid_values(self, events: list[TradeEvent]) -> None:
        count = round(len(events) * self.config.invalid_rate)
        for event in self.random.sample(events, min(count, len(events))):
            index = events.index(event)
            events[index] = replace(event, quantity=-abs(event.quantity))

    def _inject_unknown_references(self, events: list[TradeEvent]) -> None:
        count = round(len(events) * self.config.unknown_reference_rate)
        for event in self.random.sample(events, min(count, len(events))):
            index = events.index(event)
            if self.random.random() < 0.5:
                events[index] = replace(event, instrument_id="INS-UNKNOWN")
            else:
                events[index] = replace(event, portfolio_id="PORT-UNKNOWN")

    def _apply_late_delivery(self, events: Iterable[TradeEvent]) -> list[TradeEvent]:
        ordered = sorted(events, key=lambda event: (event.event_ts, event.trade_id, event.event_id))
        late_count = round(len(ordered) * self.config.late_event_rate)
        # Keep each trade's NEW event first so the MVP can focus on the important late-update
        # case: an older AMEND arriving after a newer AMEND/CANCEL. Buffering orphan events is a
        # separate production concern and intentionally outside the two-day scope.
        maximum_version = {}
        for event in ordered:
            maximum_version[event.trade_id] = max(
                maximum_version.get(event.trade_id, 0), event.event_version
            )
        eligible_indices = [
            index
            for index, event in enumerate(ordered)
            if event.event_type is EventType.AMEND
            and event.event_version < maximum_version[event.trade_id]
        ]
        late_indices = set(
            self.random.sample(eligible_indices, min(late_count, len(eligible_indices)))
        )
        self.late_events_injected = len(late_indices)
        on_time = [event for index, event in enumerate(ordered) if index not in late_indices]
        late = [event for index, event in enumerate(ordered) if index in late_indices]
        self.random.shuffle(late)
        return on_time + late


def write_generation(
    events: Iterable[TradeEvent], metadata: dict[str, object], output_dir: Path
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    event_path = output_dir / "trade_events.jsonl"
    metadata_path = output_dir / "generation_metadata.json"
    with event_path.open("w", encoding="utf-8", newline="\n") as handle:
        for replay_offset, event in enumerate(events):
            record = {"replay_offset": replay_offset, **event.to_dict()}
            handle.write(json.dumps(record, separators=(",", ":")) + "\n")
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    return event_path, metadata_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--num-trades", type=int, default=10_000)
    parser.add_argument("--duplicate-rate", type=float, default=0.01)
    parser.add_argument("--invalid-rate", type=float, default=0.005)
    parser.add_argument("--unknown-reference-rate", type=float, default=0.005)
    parser.add_argument("--late-event-rate", type=float, default=0.01)
    parser.add_argument("--conflict-rate", type=float, default=0.001)
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--event-date", default="2026-08-11")
    parser.add_argument(
        "--instruments",
        type=Path,
        default=Path("data/sample/reference/instruments.csv"),
    )
    parser.add_argument(
        "--portfolios",
        type=Path,
        default=Path("data/sample/reference/portfolios.csv"),
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data/generated"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = GenerationConfig(
        num_trades=args.num_trades,
        duplicate_rate=args.duplicate_rate,
        invalid_rate=args.invalid_rate,
        unknown_reference_rate=args.unknown_reference_rate,
        late_event_rate=args.late_event_rate,
        conflict_rate=args.conflict_rate,
        seed=args.seed,
        event_date=args.event_date,
    )
    instrument_currencies = load_instrument_currencies(args.instruments)
    generator = EventGenerator(
        config=config,
        instrument_ids=list(instrument_currencies),
        portfolio_ids=load_column(args.portfolios, "portfolio_id"),
        instrument_currencies=instrument_currencies,
    )
    events, metadata = generator.generate()
    event_path, metadata_path = write_generation(events, metadata, args.output_dir)
    print(f"Generated {len(events):,} events: {event_path}")
    print(f"Generation metadata: {metadata_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
