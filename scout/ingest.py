from __future__ import annotations

import hashlib
import sys
from pathlib import Path

from scout.chunking import chunk_text
from scout.store import Document, ScoutStore, StoredChunk


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def ingest_folder(raw_dir: Path, db_path: Path) -> None:
    if not raw_dir.exists() or not raw_dir.is_dir():
        raise SystemExit(f"Raw folder not found: {raw_dir}")

    store = ScoutStore(str(db_path))

    txt_files = sorted(raw_dir.glob("*.txt"))
    if not txt_files:
        print(f"No .txt files found in {raw_dir}")
        return

    total_chunks = 0

    for p in txt_files:
        text = p.read_text(encoding="utf-8", errors="replace")
        doc_id = file_sha256(p)  # stable ID based on file contents
        doc = Document(doc_id=doc_id, name=p.name, path=str(p.resolve()))
        store.upsert_document(doc)

        chunks = chunk_text(text, chunk_size=1000, overlap=150)
        stored = [StoredChunk(doc_id=doc_id, chunk_index=c.index, text=c.text) for c in chunks]
        store.replace_chunks(doc_id, stored)

        total_chunks += len(stored)
        print(f"Ingested {p.name}: {len(stored)} chunks")

    print("\nDone.")
    print(f"Documents: {store.count_documents()}")
    print(f"Chunks:     {store.count_chunks()}")
    preview = store.get_top_chunks(limit=10)
    if preview:
        print("\nPreview (first 10 chunks):")
        for name, idx, n in preview:
            print(f" - {name} | chunk {idx} | {n} chars")


def main() -> None:
    # Usage: python -m scout.ingest data/raw
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python -m scout.ingest <path_to_raw_folder>")

    raw_dir = Path(sys.argv[1]).resolve()
    db_path = Path("data/index/scout.db").resolve()
    ingest_folder(raw_dir, db_path)


if __name__ == "__main__":
    main()
