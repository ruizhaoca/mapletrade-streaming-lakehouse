from __future__ import annotations

import json
from collections import Counter

from mapletrade.generator import EventGenerator, GenerationConfig, write_generation
from mapletrade.models import EventType


def make_generator(seed: int = 2026) -> EventGenerator:
    return EventGenerator(
        GenerationConfig(
            num_trades=200,
            amend_rate=0.30,
            multiple_amend_rate=0.25,
            cancel_rate=0.10,
            duplicate_rate=0.05,
            invalid_rate=0.02,
            unknown_reference_rate=0.02,
            late_event_rate=0.05,
            conflict_rate=0.02,
            seed=seed,
        ),
        instrument_ids=["INS-1", "INS-2"],
        portfolio_ids=["PORT-1", "PORT-2"],
        instrument_currencies={"INS-1": "CAD", "INS-2": "USD"},
    )


def test_generator_is_deterministic() -> None:
    first, _ = make_generator().generate()
    second, _ = make_generator().generate()
    assert first == second


def test_generator_injects_expected_scenarios() -> None:
    events, metadata = make_generator().generate()
    event_ids = Counter(event.event_id for event in events)

    assert metadata["base_trade_count"] == 200
    assert {event.event_type for event in events} == {
        EventType.NEW,
        EventType.AMEND,
        EventType.CANCEL,
    }
    assert any(count > 1 for count in event_ids.values())
    assert any(event.quantity < 0 for event in events)
    assert any(
        event.instrument_id == "INS-UNKNOWN" or event.portfolio_id == "PORT-UNKNOWN"
        for event in events
    )


def test_replay_output_has_stable_offsets(tmp_path) -> None:
    events, metadata = make_generator().generate()
    event_path, metadata_path = write_generation(events, metadata, tmp_path)
    lines = [json.loads(line) for line in event_path.read_text(encoding="utf-8").splitlines()]

    assert [row["replay_offset"] for row in lines] == list(range(len(lines)))
    assert json.loads(metadata_path.read_text(encoding="utf-8"))["seed"] == 2026
