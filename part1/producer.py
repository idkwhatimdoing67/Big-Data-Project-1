#!/usr/bin/env python3
"""Continuously publish public Hacker News stories and comments to Kafka."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import fcntl
from html.parser import HTMLParser
import json
import logging
import os
from pathlib import Path
import signal
import ssl
from threading import Event
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

LOG = logging.getLogger(__name__)
BATCH_SIZE = 100
OVERLAP = 32


class APIError(Exception):
    """A failed Hacker News request that can be retried in the next poll."""


class PlainText(HTMLParser):
    BLOCKS = {"p", "br", "div", "li", "blockquote", "pre", "ul", "ol", "hr"}
    HIDDEN = {"script", "style"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.hidden = None

    def handle_starttag(self, tag, attrs):
        if self.hidden is None:
            if tag in self.HIDDEN:
                self.hidden = tag
            elif tag in self.BLOCKS:
                self.parts.append(" ")

    def handle_endtag(self, tag):
        if self.hidden == tag:
            self.hidden = None
        elif self.hidden is None and tag in self.BLOCKS:
            self.parts.append(" ")

    def handle_data(self, data):
        if self.hidden is None:
            self.parts.append(data)


def clean_html(raw):
    parser = PlainText()
    if isinstance(raw, str):
        parser.feed(raw)
        parser.close()
    return " ".join("".join(parser.parts).split())


def message_from_item(item, item_id):
    if not isinstance(item, dict) or item.get("id") != item_id:
        return None
    if item.get("dead") or item.get("deleted"):
        return None
    if item.get("type") == "story":
        # HN link stories generally have a title but no body in the API.
        title = item.get("title")
        text = " ".join(part for part in (
            " ".join(title.split()) if isinstance(title, str) else "",
            clean_html(item.get("text")),
        ) if part)
    elif item.get("type") == "comment":
        text = clean_html(item.get("text"))
    else:
        return None
    return {"id": str(item_id), "text": text, "source": "hackernews"} if text else None


class HackerNews:
    def __init__(self, base_url, timeout):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.tls = ssl.create_default_context()

    def get(self, endpoint):
        request = Request(f"{self.base_url}/{endpoint}", headers={
            "User-Agent": "CS4371-HackerNews-Producer/1.0", "Accept": "application/json",
        })
        for attempt in range(3):
            try:
                with urlopen(request, timeout=self.timeout, context=self.tls) as response:
                    return json.load(response)
            except (OSError, ValueError) as exc:
                if isinstance(exc, HTTPError) and exc.code < 500 and exc.code != 429:
                    raise APIError(f"{endpoint}: {exc}") from exc
                if attempt == 2:
                    raise APIError(f"{endpoint}: {exc}") from exc
                time.sleep(0.5 * 2 ** attempt)

    def latest(self):
        latest = self.get("maxitem.json")
        if type(latest) is not int or latest < 1:
            raise APIError(f"Invalid Hacker News maxitem: {latest!r}")
        return latest

    def fetch(self, item_id):
        return message_from_item(self.get(f"item/{item_id}.json"), item_id)


def read_ids(output):
    ids = set()
    for line_no, line in enumerate(output, 1):
        try:
            record = json.loads(line)
            item_id = record["id"]
            if (
                set(record) != {"id", "text", "source"}
                or not isinstance(item_id, str) or not item_id.isascii() or not item_id.isdecimal()
                or not isinstance(record["text"], str) or not record["text"].strip()
                or record["source"] != "hackernews" or item_id in ids
                or not line.endswith("\n")
            ):
                raise ValueError("invalid, duplicate, or incomplete message")
        except (ValueError, TypeError, KeyError) as exc:
            raise ValueError(f"Invalid JSONL record on line {line_no}: {exc}") from exc
        ids.add(item_id)
    return ids


def publish_batch(producer, topic, messages, output, seen):
    acknowledgments = []
    errors = []

    def delivered(error, kafka_message):
        if error is None:
            acknowledgments.append(kafka_message.value())
        else:
            errors.append(str(error))

    for message in messages:
        payload = json.dumps(message, ensure_ascii=False, separators=(",", ":"))
        producer.produce(topic, key=message["id"], value=payload, on_delivery=delivered)
    remaining = producer.flush(35)
    for payload in acknowledgments:
        output.write(payload.decode("utf-8") + "\n")
        seen.add(json.loads(payload)["id"])
    if acknowledgments:
        output.flush()
        os.fsync(output.fileno())
    if remaining or errors:
        raise RuntimeError(f"Kafka delivery failed ({remaining} pending): {'; '.join(errors[:3])}")


def options():
    parser = argparse.ArgumentParser(description=__doc__)
    host = os.getenv("KAFKA_ADVERTISED_HOST", "127.0.0.1")
    port = os.getenv("KAFKA_HOST_PORT", "9092")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", f"{host}:{port}"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC1", "topic1"))
    parser.add_argument("--output", type=Path, default=os.getenv("COLLECTED_MESSAGES_PATH", "outputs/collected_messages.jsonl"))
    parser.add_argument("--api-base-url", default=os.getenv("HN_API_BASE_URL", "https://hacker-news.firebaseio.com/v0"))
    parser.add_argument("--backfill-items", type=int, default=os.getenv("HN_BACKFILL_ITEMS", "3000"))
    parser.add_argument("--poll-seconds", type=float, default=os.getenv("HN_POLL_SECONDS", "10"))
    parser.add_argument("--workers", type=int, default=os.getenv("HN_FETCH_WORKERS", "16"))
    parser.add_argument("--http-timeout", type=float, default=os.getenv("HN_HTTP_TIMEOUT_SECONDS", "10"))
    parser.add_argument("--limit", type=int, help="Stop when the output contains this many unique messages in total.")
    args = parser.parse_args()
    for name in ("backfill_items", "poll_seconds", "workers", "http_timeout", "limit"):
        value = getattr(args, name)
        if value is not None and value <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    return args


def collect(args, producer, api, output, seen, stop):
    # Start near the current high-water mark; then follow newly assigned item IDs.
    cursor = None
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        while not stop.is_set():
            if args.limit is not None and len(seen) >= args.limit:
                return
            try:
                latest = api.latest()
                if cursor is None:
                    cursor = max(1, latest - args.backfill_items + 1)
                start = min(cursor, max(1, latest - OVERLAP + 1))
                for low in range(start, latest + 1, BATCH_SIZE):
                    if stop.is_set():
                        return
                    high = min(low + BATCH_SIZE, latest + 1)
                    item_ids = (i for i in range(low, high) if str(i) not in seen)
                    batch = [message for message in pool.map(api.fetch, item_ids) if message]
                    if args.limit is not None:
                        batch = batch[:args.limit - len(seen)]
                    if batch:
                        publish_batch(producer, args.topic, batch, output, seen)
                        LOG.info("Published and saved %d messages (%d total)", len(batch), len(seen))
                    cursor = max(cursor, high)
                    if args.limit is not None and len(seen) >= args.limit:
                        return
            except APIError as exc:
                LOG.warning("Hacker News poll interrupted; will retry: %s", exc)
            stop.wait(args.poll_seconds)


def main():
    from confluent_kafka import Producer

    args = options()
    stop = Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("a+", encoding="utf-8", newline="\n") as output:
        fcntl.flock(output, fcntl.LOCK_EX | fcntl.LOCK_NB)
        output.seek(0)
        seen = read_ids(output)
        LOG.info("Loaded %d existing IDs from %s", len(seen), args.output)
        if args.limit is not None and len(seen) >= args.limit:
            return
        producer = Producer({
            "bootstrap.servers": args.bootstrap_servers,
            "enable.idempotence": True,
            "delivery.timeout.ms": 30000,
        })
        metadata = producer.list_topics(timeout=10)
        if args.topic not in metadata.topics or metadata.topics[args.topic].error:
            raise RuntimeError(f"Kafka topic {args.topic!r} is unavailable; start kafka-init first")
        collect(args, producer, HackerNews(args.api_base_url, args.http_timeout), output, seen, stop)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        raise SystemExit(f"Producer stopped: {exc}") from exc
