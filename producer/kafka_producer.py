from __future__ import annotations

import argparse
import json
import os
from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path
from typing import Any

REQUIRED_ENVIRONMENT_VARIABLES = (
    "CONFLUENT_BOOTSTRAP_SERVERS",
    "CONFLUENT_KAFKA_API_KEY",
    "CONFLUENT_KAFKA_API_SECRET",
    "SCHEMA_REGISTRY_URL",
    "SCHEMA_REGISTRY_API_KEY",
    "SCHEMA_REGISTRY_API_SECRET",
)


def required_environment() -> dict[str, str]:
    missing = [name for name in REQUIRED_ENVIRONMENT_VARIABLES if not os.getenv(name)]
    if missing:
        raise RuntimeError("Missing required environment variables: " + ", ".join(missing))
    return {name: os.environ[name] for name in REQUIRED_ENVIRONMENT_VARIABLES}


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def publish(events_path: Path, schema_path: Path, topic: str) -> int:
    try:
        from confluent_kafka import SerializingProducer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import StringSerializer
    except ImportError as exc:
        raise RuntimeError(
            "Install the producer dependencies with: pip install -e .[producer]"
        ) from exc

    settings = required_environment()
    schema_string = schema_path.read_text(encoding="utf-8")
    registry = SchemaRegistryClient(
        {
            "url": settings["SCHEMA_REGISTRY_URL"],
            "basic.auth.user.info": (
                f"{settings['SCHEMA_REGISTRY_API_KEY']}:{settings['SCHEMA_REGISTRY_API_SECRET']}"
            ),
        }
    )

    def to_avro(record: dict[str, Any], _context: Any) -> dict[str, Any]:
        converted = {key: value for key, value in record.items() if key != "replay_offset"}
        converted["price"] = Decimal(str(converted["price"]))
        return converted

    producer = SerializingProducer(
        {
            "bootstrap.servers": settings["CONFLUENT_BOOTSTRAP_SERVERS"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": settings["CONFLUENT_KAFKA_API_KEY"],
            "sasl.password": settings["CONFLUENT_KAFKA_API_SECRET"],
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": AvroSerializer(registry, schema_string, to_avro),
            "enable.idempotence": True,
            "acks": "all",
        }
    )

    failures: list[str] = []

    def delivered(error: Any, message: Any) -> None:
        if error is not None:
            failures.append(str(error))

    events = load_jsonl(events_path)
    for event in events:
        producer.produce(
            topic=topic,
            key=event["trade_id"],
            value=event,
            on_delivery=delivered,
        )
        producer.poll(0)
    remaining = producer.flush(30)
    if remaining or failures:
        raise RuntimeError(
            f"Kafka publish incomplete: {remaining} queued messages; {len(failures)} failures"
        )
    print(f"Published {len(events):,} events to {topic}")
    return len(events)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, default=Path("data/generated/trade_events.jsonl"))
    parser.add_argument("--schema", type=Path, default=Path("schemas/trade_event_v1.avsc"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "mapletrade.trade-events.v1"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    publish(args.events, args.schema, args.topic)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
