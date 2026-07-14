#!/usr/bin/env python3
"""Kafka consumer for the `signals` topic.

    pip install kafka-python
    python kafka_consumer.py [localhost:29092]

Note: from your host machine use localhost:29092 (the EXTERNAL listener).
Containers on the compose network use kafka:9092 instead.
"""
import json
import sys

from kafka import KafkaConsumer

broker = sys.argv[1] if len(sys.argv) > 1 else "localhost:29092"

consumer = KafkaConsumer(
    "signals",
    bootstrap_servers=broker,
    auto_offset_reset="latest",
    group_id="intern-demo",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

print(f"consuming 'signals' from {broker} — ctrl-c to stop", file=sys.stderr)
for record in consumer:
    s = record.value
    print(f"p{record.partition}@{record.offset}", s["type"], s["service"], s["severity"])
