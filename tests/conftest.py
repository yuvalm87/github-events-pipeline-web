import os
import sys
import importlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(tmp_path, monkeypatch):
    """Create a TestClient with DATA_DIR pointed to a temp directory.

    This fixture ensures the application modules are re-imported after
    setting the environment so they pick up the test DATA_DIR.
    """
    # Ensure repository root is on sys.path so `import app` works in tests
    repo_root = Path(__file__).resolve().parents[1]
    repo_root_str = str(repo_root)
    if repo_root_str not in sys.path:
        sys.path.insert(0, repo_root_str)

    data_dir = tmp_path / "data"
    monkeypatch.setenv("DATA_DIR", str(data_dir))

    # Remove app modules so they re-read env vars on import
    for mod in list(sys.modules.keys()):
        if mod.startswith("app"):
            del sys.modules[mod]

    app_module = importlib.import_module("app.main")
    client = TestClient(app_module.app)

    yield client
