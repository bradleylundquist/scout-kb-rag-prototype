from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional


@dataclass(frozen=True)
class Document:
    doc_id: str
    name: str
    path: str


@dataclass(frozen=True)
class StoredChunk:
    doc_id: str
    chunk_index: int
    text: str


class ScoutStore:
    
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(os.path.dirname(db_path)).mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS documents (
                    doc_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    path TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS chunks (
                    doc_id TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    text TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (doc_id, chunk_index),
                    FOREIGN KEY (doc_id) REFERENCES documents(doc_id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_chunks_doc_id ON chunks(doc_id);
                """
            )

    def upsert_document(self, doc: Document) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO documents (doc_id, name, path, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(doc_id) DO UPDATE SET
                    name=excluded.name,
                    path=excluded.path
                """,
                (doc.doc_id, doc.name, doc.path, now),
            )

    def replace_chunks(self, doc_id: str, chunks: Iterable[StoredChunk]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute("DELETE FROM chunks WHERE doc_id = ?", (doc_id,))
            conn.executemany(
                """
                INSERT INTO chunks (doc_id, chunk_index, text, created_at)
                VALUES (?, ?, ?, ?)
                """,
                [(c.doc_id, c.chunk_index, c.text, now) for c in chunks],
            )

    def count_documents(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) FROM documents").fetchone()
            return int(row[0]) if row else 0

    def count_chunks(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) FROM chunks").fetchone()
            return int(row[0]) if row else 0

    def search_chunks(self, query: str, *, limit: int = 5) -> list[tuple[str, int, str]]:
        """
        Simple keyword search using SQLite LIKE.
        Returns: [(doc_name, chunk_index, chunk_text), ...]
        """
        q = query.strip()
        if not q:
            return []

        like = f"%{q}%"
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT d.name, c.chunk_index, c.text
                FROM chunks c
                JOIN documents d ON d.doc_id = c.doc_id
                WHERE c.text LIKE ?
                ORDER BY d.name ASC, c.chunk_index ASC
                LIMIT ?
                """,
                (like, limit),
            ).fetchall()

        return [(r[0], int(r[1]), str(r[2])) for r in rows]

    def get_top_chunks(self, *, limit: int = 5) -> list[tuple[str, int, int]]:
        """
        Returns a quick view: [(doc_name, chunk_index, chunk_char_len), ...]
        """
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT d.name, c.chunk_index, LENGTH(c.text) as n
                FROM chunks c
                JOIN documents d ON d.doc_id = c.doc_id
                ORDER BY d.name ASC, c.chunk_index ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [(r[0], int(r[1]), int(r[2])) for r in rows]
