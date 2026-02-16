from __future__ import annotations

import sys
from pathlib import Path

from scout.store import ScoutStore


def main() -> None:
    # Usage: py -m scout.search "hipaa"
    if len(sys.argv) < 2:
        raise SystemExit('Usage: py -m scout.search "<query>"')

    query = sys.argv[1]
    db_path = Path("data/index/scout.db").resolve()
    store = ScoutStore(str(db_path))

    results = store.search_chunks(query, limit=10)

    if not results:
        print(f'No matches for: "{query}"')
        return

    print(f'\nResults for: "{query}"\n')
    for doc_name, chunk_index, text in results:
        snippet = text.replace("\n", " ").strip()
        if len(snippet) > 180:
            snippet = snippet[:180] + "..."
        print(f"- {doc_name} | chunk {chunk_index} | {snippet}")


if __name__ == "__main__":
    main()
