import json
import logging

logger = logging.getLogger(__name__)


def load_config(path: str = "config.json") -> dict:
    """Đọc file config.json và trả về dict."""
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.critical(f"Không tìm thấy file config: {path}")
        raise
    except json.JSONDecodeError as e:
        logger.critical(f"config.json bị lỗi cú pháp: {e}")
        raise
