from __future__ import annotations

import os
import asyncio
from .models import ChunkRecord

class LiveManifest:
    def __init__(self, path: str) -> None:
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        open(self.path, "a").close()
        self._lock = asyncio.Lock()

    async def append(self, rec: ChunkRecord) -> None:
        line = rec.to_json_line()
        async with self._lock:
            await asyncio.to_thread(self._write_line, self.path, line)

    @staticmethod
    def _write_line(path: str, line: str) -> None:
        with open(path, "a", buffering=1) as f:
            f.write(line); f.flush(); os.fsync(f.fileno())
