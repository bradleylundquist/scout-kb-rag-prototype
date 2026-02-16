from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class Chunk:
    text: str
    index: int


def chunk_text(text: str, *, chunk_size: int = 150, overlap: int = 30) -> List[Chunk]:
    """
    Simple character-based chunking with overlap.
    Keeps MVP easy to reason about and debug.

    chunk_size: target characters per chunk
    overlap: characters of overlap between chunks
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    if overlap < 0:
        raise ValueError("overlap must be >= 0")
    if overlap >= chunk_size:
        raise ValueError("overlap must be < chunk_size")

    text = text.replace("\r\n", "\n").strip()
    if not text:
        return []

    chunks: List[Chunk] = []
    start = 0
    i = 0

    while start < len(text):
        end = min(start + chunk_size, len(text))
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(Chunk(text=chunk, index=i))
            i += 1

        if end == len(text):
            break

        start = end - overlap

    return chunks
