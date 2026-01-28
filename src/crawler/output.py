"""Streaming output writer for crawl results."""

import json
from pathlib import Path
from typing import TextIO


class StreamingOutputWriter:
    """Writes crawl results to JSONL format one at a time."""

    def __init__(
        self,
        output_path: str | Path,
        include_content: bool = True,
    ):
        self.output_path = Path(output_path)
        self.include_content = include_content
        self._file: TextIO | None = None
        self._count = 0

    def __enter__(self) -> "StreamingOutputWriter":
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.output_path, "w", encoding="utf-8")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._file is not None:
            self._file.close()
            self._file = None

    def write_one(self, result: dict):
        """Write a single result to the output file."""
        if self._file is None:
            raise RuntimeError("StreamingOutputWriter must be used as context manager")

        output = result.copy()
        if not self.include_content and "content" in output:
            del output["content"]

        self._file.write(json.dumps(output, ensure_ascii=False) + "\n")
        self._file.flush()
        self._count += 1

    @property
    def count(self) -> int:
        """Number of results written."""
        return self._count
