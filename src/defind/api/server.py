from __future__ import annotations

from dotenv import load_dotenv

from defind.api.ops_api import create_app, load_ops_api_config_from_env
from defind.observability import configure_logging


def main() -> None:
    load_dotenv()
    cfg = load_ops_api_config_from_env()
    configure_logging(
        level=cfg.log_level,
        json_logs=cfg.log_json,
        json_log_file_path=cfg.json_log_file_path,
    )
    app = create_app(cfg)

    try:
        import uvicorn
    except Exception as exc:  # pragma: no cover - runtime dependency check
        raise RuntimeError("uvicorn is required to run ops API: pip install uvicorn fastapi") from exc

    uvicorn.run(app, host=cfg.host, port=cfg.port, log_config=None)
