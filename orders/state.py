"""Persist each accepted Kafka record so offset replay cannot inflate the average."""

import sqlite3
from pathlib import Path


class Aggregate:
    def __init__(self, path):
        if str(path) != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS accepted (
                topic TEXT NOT NULL,
                partition_id INTEGER NOT NULL,
                offset_id INTEGER NOT NULL,
                PRIMARY KEY (topic, partition_id, offset_id)
            );
            CREATE TABLE IF NOT EXISTS totals (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                count INTEGER NOT NULL,
                total REAL NOT NULL
            );
            INSERT OR IGNORE INTO totals VALUES (1, 0, 0);
        """)

    def add(self, source, price):
        # The receipt and total change commit together. A crash before the Kafka
        # offset commit can replay this record, but cannot count it twice here.
        with self.db:
            inserted = self.db.execute(
                "INSERT OR IGNORE INTO accepted VALUES (?, ?, ?)", source
            ).rowcount
            if inserted:
                self.db.execute("UPDATE totals SET count=count+1, total=total+? WHERE id=1", (price,))
        return bool(inserted)

    def snapshot(self):
        count, total = self.db.execute("SELECT count, total FROM totals WHERE id=1").fetchone()
        return {"count": count, "total": total, "average": total / count if count else 0.0}

    def close(self):
        self.db.close()
