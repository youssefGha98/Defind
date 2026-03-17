from __future__ import annotations

import logging
from pathlib import Path

from defind.observability import TextLogFormatter, configure_logging


def test_text_log_formatter_appends_structured_extras() -> None:
    formatter = TextLogFormatter()
    record = logging.makeLogRecord(
        {
            "name": "defind.test",
            "levelno": logging.INFO,
            "levelname": "INFO",
            "msg": "chunk_manifest",
            "args": (),
            "from_block": 140099995,
            "to_block": 140119993,
            "status": "started",
            "attempts": 0,
            "error": None,
            "logs": 0,
            "decoded": 0,
            "shards": 0,
            "updated_at": 1764935583.7648973,
            "filtered": 0,
        }
    )

    rendered = formatter.format(record)

    assert "chunk_manifest" in rendered
    assert '"from_block":140099995' in rendered
    assert '"to_block":140119993' in rendered
    assert '"status":"started"' in rendered
    assert '"attempts":0' in rendered
    assert '"error":null' in rendered
    assert '"updated_at":1764935583.7648973' in rendered


def test_configure_logging_reformats_existing_root_handlers() -> None:
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    root.handlers = [handler]

    try:
        configure_logging(level="INFO", json_logs=False)
        assert isinstance(root.handlers[0].formatter, TextLogFormatter)
    finally:
        root.handlers = original_handlers
        root.setLevel(original_level)


def test_configure_logging_adds_json_file_handler(tmp_path: Path) -> None:
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    log_path = tmp_path / "runtime" / "logs" / "defind-api.jsonl"

    try:
        root.handlers = []
        configure_logging(level="INFO", json_logs=False, json_log_file_path=log_path)
        root.info("hello", extra={"dataset_id": "uniswap/usdc_weth", "job_id": "job-1"})
    finally:
        for handler in root.handlers:
            handler.flush()
            handler.close()
        root.handlers = original_handlers
        root.setLevel(original_level)

    raw = log_path.read_text(encoding="utf-8")
    assert '"event":"hello"' in raw
    assert '"dataset_id":"uniswap/usdc_weth"' in raw
    assert '"job_id":"job-1"' in raw
