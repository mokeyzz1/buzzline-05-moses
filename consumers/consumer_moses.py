"""
consumer_moses.py

Custom consumer (Kafka → SQLite).
Uses the existing JSON 'category' as the topic and stores one row per message.

Expected message shape:
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

Writes to SQLite table 'message_topics' in data/topics_moses.sqlite
Schema (see db_sqlite_moses.py):
- id (INTEGER PRIMARY KEY AUTOINCREMENT)
- timestamp (TEXT)
- topic (TEXT)
- raw_message (TEXT)
"""

# standard library
import json
import os
import pathlib
import sys

# external
from kafka import KafkaConsumer

# local modules
import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

# make package imports work when run as module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from consumers.db_sqlite_moses import init_db, insert_message  # our DB helpers


# ---------- processing for ONE message ----------
def process_message(msg: dict) -> dict | None:
    """
    Take raw JSON dict and return the minimal fields we want to store.
    Uses existing 'category' as the topic; falls back to 'keyword_mentioned' or 'other'.
    """
    if not isinstance(msg, dict):
        logger.warning("Skipping non-dict message value.")
        return None

    topic = (msg.get("category") or msg.get("keyword_mentioned") or "other")
    topic = str(topic).strip().lower()

    processed = {
        "timestamp": msg.get("timestamp"),
        "topic": topic,
        "raw_message": msg.get("message"),
    }
    logger.info(f"Processed message → {processed}")
    return processed


def consume_messages_from_kafka(topic: str, group: str, sqlite_path: pathlib.Path):
    """
    Subscribe to topic, process messages one-by-one, insert into SQLite.
    """
    logger.info("Called consume_messages_from_kafka()")
    logger.info(f"  topic='{topic}', group='{group}', sqlite_path='{sqlite_path}'")

    # create consumer with JSON deserializer — consistent with sample utils
    consumer: KafkaConsumer = create_kafka_consumer(
        topic,
        group,
        value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
    )
    logger.info("Kafka consumer created successfully.")
    logger.info("Consuming messages now...")

    for record in consumer:
        try:
            processed = process_message(record.value)
            if processed:
                insert_message(processed, sqlite_path)
        except KeyboardInterrupt:
            logger.warning("Interrupted by user.")
            break
        except Exception as e:
            logger.error(f"Error handling record: {e}")


def main():
    """
    Read env, create DB (own file), and start consuming from Kafka.
    """
    logger.info("Starting consumer_moses (Kafka → SQLite, topic via 'category').")

    # 1) env/config (keep identical style to professor code)
    topic = config.get_kafka_topic()
    group_id = config.get_kafka_consumer_group_id()
    base_dir: pathlib.Path = config.get_base_data_path()


    # Use our OWN sqlite filename so we don't clash with the default DB
    sqlite_path = base_dir / "topics_moses.sqlite"
    base_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"BUZZ_TOPIC: {topic}")
    logger.info(f"BUZZ_CONSUMER_GROUP_ID: {group_id}")
    logger.info(f"BASE_DATA_DIR: {base_dir}")
    logger.info(f"SQLITE_PATH: {sqlite_path}")

    # 2) (Optional) fresh start for THIS DB only
    if sqlite_path.exists():
        try:
            sqlite_path.unlink()
            logger.info("Old topics_moses.sqlite removed for a clean start.")
        except Exception as e:
            logger.error(f"Couldn't remove old DB: {e}")

    # 3) init DB/table
    init_db(sqlite_path)

    # 4) consume loop
    consume_messages_from_kafka(topic, group_id, sqlite_path)


if __name__ == "__main__":
    main()
