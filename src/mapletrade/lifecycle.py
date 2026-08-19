from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from mapletrade.models import EventType, Outcome, TradeEvent, TradeStatus
from mapletrade.quality import payload_hash


@dataclass(frozen=True, slots=True)
class TradeState:
    trade_id: str
    current_version: int
    trade_status: TradeStatus
    payload_hash: str


@dataclass(frozen=True, slots=True)
class EventDecision:
    event_id: str
    trade_id: str
    event_version: int
    outcome: Outcome
    reason: str


def classify_lifecycle(
    events: Iterable[TradeEvent],
    initial_states: dict[str, TradeState] | None = None,
    seen_event_ids: set[str] | None = None,
) -> tuple[list[EventDecision], dict[str, TradeState]]:
    """Reference lifecycle implementation used by unit tests and documentation examples."""
    states = dict(initial_states or {})
    seen = set(seen_event_ids or set())
    decisions: list[EventDecision] = []

    for event in events:
        event_payload_hash = payload_hash(event.to_dict())
        state = states.get(event.trade_id)

        if event.event_id in seen:
            decision = EventDecision(
                event.event_id,
                event.trade_id,
                event.event_version,
                Outcome.DUPLICATE,
                "DUPLICATE_EVENT_ID",
            )
        elif state and event.event_version < state.current_version:
            decision = EventDecision(
                event.event_id,
                event.trade_id,
                event.event_version,
                Outcome.STALE_VERSION,
                "LOWER_THAN_CURRENT_VERSION",
            )
        elif state and event.event_version == state.current_version:
            same_payload = event_payload_hash == state.payload_hash
            decision = EventDecision(
                event.event_id,
                event.trade_id,
                event.event_version,
                Outcome.DUPLICATE if same_payload else Outcome.VERSION_CONFLICT,
                "REPLAYED_TRADE_VERSION" if same_payload else "CONFLICTING_TRADE_VERSION",
            )
        elif state is None and not (event.event_type is EventType.NEW and event.event_version == 1):
            decision = EventDecision(
                event.event_id,
                event.trade_id,
                event.event_version,
                Outcome.QUARANTINED,
                "LIFECYCLE_MUST_START_WITH_NEW_VERSION_1",
            )
        elif state and state.trade_status is TradeStatus.CANCELLED:
            decision = EventDecision(
                event.event_id,
                event.trade_id,
                event.event_version,
                Outcome.QUARANTINED,
                "EVENT_AFTER_CANCEL",
            )
        elif state and event.event_type is EventType.NEW:
            decision = EventDecision(
                event.event_id,
                event.trade_id,
                event.event_version,
                Outcome.QUARANTINED,
                "NEW_FOR_EXISTING_TRADE",
            )
        else:
            status = (
                TradeStatus.CANCELLED
                if event.event_type is EventType.CANCEL
                else TradeStatus.BOOKED
            )
            states[event.trade_id] = TradeState(
                trade_id=event.trade_id,
                current_version=event.event_version,
                trade_status=status,
                payload_hash=event_payload_hash,
            )
            decision = EventDecision(
                event.event_id,
                event.trade_id,
                event.event_version,
                Outcome.APPLIED,
                "ACCEPTED_HIGHER_VERSION",
            )

        seen.add(event.event_id)
        decisions.append(decision)

    return decisions, states
