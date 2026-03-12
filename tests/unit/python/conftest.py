# conftest.py — picked up automatically by pytest.
# Ensures the Python client package is importable when tests are run from the
# repo root (e.g. `python3 -m pytest tests/unit/python/`).
import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[3]
_client_path = str(_repo_root / "client" / "python")
if _client_path not in sys.path:
    sys.path.insert(0, _client_path)
