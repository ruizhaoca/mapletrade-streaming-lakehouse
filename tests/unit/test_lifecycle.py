from __future__ import annotations

from dataclasses import replace

from mapletrade.lifecycle import classify_lifecycle
from mapletrade.models import EventType, Outcome, Side, TradeEvent, TradeStatus


def event(
    event_id: str,
    trade_id: str,
    version: int,
    event_type: EventType,
    *,
    price: str = "10.0000",
) -> TradeEvent:
    return TradeEvent(
        event_id=event_id,
        trade_id=trade_id,
        event_version=version,
        event_type=event_type,
        event_ts=1_786_454_400_000 + version * 1_000,
        produced_ts=1_786_454_410_000 + version * 1_000,
        source_system="SYNTHETIC_OMS",
        portfolio_id="PORT-001",
        instrument_id="INS-001",
        side=Side.BUY,
        quantity=100,
        price=price,
        trade_currency="CAD",
    )


def test_new_amend_cancel_sequence_updates_current_state() -> None:
    events = [
        event("E1", "T1", 1, EventType.NEW),
        event("E2", "T1", 2, EventType.AMEND),
        event("E3", "T1", 3, EventType.CANCEL),
    ]
    decisions, states = classify_lifecycle(events)

    assert [decision.outcome for decision in decisions] == [Outcome.APPLIED] * 3
    assert states["T1"].current_version == 3
    assert states["T1"].trade_status is TradeStatus.CANCELLED


def test_duplicate_stale_conflict_and_post_cancel_are_rejected() -> None:
    new = event("E1", "T1", 1, EventType.NEW)
    amend = event("E2", "T1", 2, EventType.AMEND)
    duplicate = replace(amend)
    stale = event("E3", "T1", 1, EventType.NEW)
    conflict = event("E4", "T1", 2, EventType.AMEND, price="11.0000")
    cancel = event("E5", "T1", 3, EventType.CANCEL)
    after_cancel = event("E6", "T1", 4, EventType.AMEND)

    decisions, states = classify_lifecycle(
        [new, amend, duplicate, stale, conflict, cancel, after_cancel]
    )

    assert [decision.outcome for decision in decisions] == [
        Outcome.APPLIED,
        Outcome.APPLIED,
        Outcome.DUPLICATE,
        Outcome.STALE_VERSION,
        Outcome.VERSION_CONFLICT,
        Outcome.APPLIED,
        Outcome.QUARANTINED,
    ]
    assert states["T1"].trade_status is TradeStatus.CANCELLED


def test_lifecycle_must_start_with_new_version_one() -> None:
    decisions, states = classify_lifecycle([event("E1", "T1", 2, EventType.AMEND)])
    assert decisions[0].outcome is Outcome.QUARANTINED
    assert states == {}
