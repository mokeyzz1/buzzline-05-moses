"""
consumer_moses.py

Custom Kafka consumer that assigns a normalized topic to each message
(based on the message text) and stores it in SQLite.

This follows the same structure as kafka_consumer_case.py so it fits the repo.
It keeps the same DB insert API (from sqlite_consumer_case.py), but replaces
the "category" field with our own normalized topic.
"""

# -------------------------------
# Imports (match project style)
# -------------------------------
import json
import os
import pathlib
import sys

from kafka import KafkaConsumer

import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available  # keep parity

# Make parent importable (same pattern as case file)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from consumers.sqlite_consumer_case import init_db, insert_message


# -------------------------------
# Topic classification (simple)
# -------------------------------
def classify_topic(text: str) -> str:
    """Return one of: Sports, Tech, Music, News, Other."""
    if not text:
        return "Other"
    t = text.lower()
    if any(w in t for w in ("game", "score", "team", "win", "season", "coach")):
        return "Sports"
    if any(w in t for w in ("tech", "ai", "software", "code", "app", "device")):
        return "Tech"
    if any(w in t for w in ("music", "song", "album", "concert", "track", "band")):
        return "Music"
    if any(w in t for w in ("news", "report", "election", "breaking", "headline")):
        return "News"
    return "Other"


# -------------------------------
# Process ONE message
# -------------------------------
def process_message(message: dict):
    """
    Transform one JSON message into the shape expected by insert_message(),
    but with our own normalized topic in the 'category' field.
    """
    logger.info("Called process_message() with:")
    logger.info(f"   {message=}")

    try:
        raw = message.get("message", "")
        processed = {
            "message": raw,
            "author": message.get("author"),
            "timestamp": message.get("timestamp"),
            # >>> our normalized topic goes here:
            "category": classify_topic(raw),
            # keep remaining fields so insert_message() signature stays happy
            "sentiment": float(message.get("sentiment", 0.0)),
            "keyword_mentioned": message.get("keyword_mentioned"),
            "message_length": int(message.get("message_length", len(raw or ""))),
        }
        logger.info(f"Processed message: {processed}")
        return processed
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None


# -------------------------------
# Consume from Kafka
# -------------------------------
def consume_messages_from_kafka(
    topic: str,
    kafka_url: str,
    group: str,
    sql_path: pathlib.Path,
    interval_secs: int,
):
    logger.info("Called consume_messages_from_kafka() with:")
    logger.info(f"   {topic=}")
    logger.info(f"   {kafka_url=}")
    logger.info(f"   {group=}")
    logger.info(f"   {sql_path=}")
    logger.info(f"   {interval_secs=}")

    logger.info("Step 1. Verify Kafka Services.")
    verify_services()

    logger.info("Step 2. Create a Kafka consumer.")
    consumer: KafkaConsumer = create_kafka_consumer(
        topic,
        group,
        value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
    )

    logger.info("Step 3. Verify topic exists.")
    is_topic_available(topic)  # will raise if not available
    logger.info(f"Kafka topic '{topic}' is ready.")

    logger.info("Step 4. Process messages.")
    for record in consumer:
        processed = process_message(record.value)
        if processed:
            insert_message(processed, sql_path)


# -------------------------------
# Main
# -------------------------------
def main():
    logger.info("Starting consumer_moses (Kafka → SQLite, topic normalization).")

    # Read env via utils (same as case file)
    topic = config.get_kafka_topic()
    kafka_url = config.get_kafka_broker_address()
    group_id = config.get_kafka_consumer_group_id()
    interval_secs: int = config.get_message_interval_seconds_as_int()
    sqlite_path: pathlib.Path = config.get_sqlite_path()

    # Fresh DB (same behavior as case file)
    if sqlite_path.exists():
        sqlite_path.unlink()

    init_db(sqlite_path)

    try:
        consume_messages_from_kafka(
            topic, kafka_url, group_id, sqlite_path, interval_secs
        )
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    finally:
        logger.info("consumer_moses shutting down.")


if __name__ == "__main__":
    main()
