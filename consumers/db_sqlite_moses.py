"""
db_sqlite_moses.py

SQLite database functions for consumer_moses.
"""

import pathlib
import sqlite3
from utils.utils_logger import logger


def init_db(db_path: pathlib.Path):
    """Create table message_topics if not exists."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS message_topics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                topic TEXT,
                raw_message TEXT
            )
            """
        )
        conn.commit()
        conn.close()
        logger.info(f"SUCCESS: Database initialized at {db_path}")
    except Exception as e:
        logger.error(f"DB init failed: {e}")
        raise


def insert_message(message: dict, db_path: pathlib.Path):
    """Insert one processed message into message_topics table."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO message_topics (timestamp, topic, raw_message)
            VALUES (?, ?, ?)
            """,
            (
                message.get("timestamp"),
                message.get("topic"),
                message.get("raw_message"),
            ),
        )
        conn.commit()
        conn.close()
        logger.info(f"Inserted into DB: {message}")
    except Exception as e:
        logger.error(f"Insert failed: {e}")
        raise
