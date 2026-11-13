"""Analytics consumer entry point."""

from __future__ import annotations

import logging
import os
import signal
import time

import boto3

from analytics.chaos import Chaos
from analytics.consumer import Consumer
from analytics.store import PostgresStore, RedisSeenCache

CONSUMER_NAME = "analytics"


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format='{"time":"%(asctime)s","level":"%(levelname)s","service":"analytics",'
        '"msg":"%(message)s"}',
    )
    log = logging.getLogger("analytics")

    chaos = Chaos(os.environ.get("CHAOS", ""))
    store = PostgresStore(os.environ["DATABASE_URL"])
    consumer = Consumer(
        name=CONSUMER_NAME,
        queue=boto3.client("sqs"),  # honours AWS_ENDPOINT_URL for local runs
        queue_url=os.environ["QUEUE_URL"],
        store=store,
        cache=RedisSeenCache(os.environ["REDIS_URL"], CONSUMER_NAME),
        chaos=chaos,
    )

    running = True

    def stop(*_: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("consuming %s (chaos=%s)", consumer.queue_url, chaos.enabled)
    last_report = time.monotonic()
    while running:
        try:
            consumer.poll_once()
        except Exception:
            log.exception("receive failed, retrying")
            time.sleep(1)
        if time.monotonic() - last_report > 10:
            log.info("stats %s", dict(consumer.stats))
            last_report = time.monotonic()

    store.close()
    log.info("stopped %s", dict(consumer.stats))
