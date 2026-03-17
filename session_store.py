"""
session_store.py — зберігання даних сесії у тимчасових файлах.
"""

import json
import logging
import time
import uuid
import tempfile
from pathlib import Path

TMP_DIR = Path(tempfile.gettempdir()) / 'app_ser_sessions'
TMP_DIR.mkdir(exist_ok=True)


def save_session_data(data: dict) -> str:
    """Save data to a temp file and return a session_id UUID."""
    session_id = str(uuid.uuid4())
    path = TMP_DIR / f"{session_id}.json"
    path.write_text(json.dumps(data, default=str, ensure_ascii=False))
    path.chmod(0o600)
    return session_id


def load_session_data(session_id: str) -> dict | None:
    """Load data from a temp file by session_id."""
    if not session_id:
        return None
    # Validate UUID format to prevent path traversal
    try:
        uuid.UUID(session_id)
    except ValueError:
        return None
    path = TMP_DIR / f"{session_id}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        logging.warning(f"Failed to load session file {path}: {e}")
        return None


def cleanup_old_sessions(max_age_hours: int = 2) -> None:
    """Remove session temp files older than max_age_hours."""
    now = time.time()
    for f in TMP_DIR.glob('*.json'):
        try:
            if now - f.stat().st_mtime > max_age_hours * 3600:
                f.unlink(missing_ok=True)
        except OSError:
            pass
