"""pytest 夹具：每个测试使用独立的临时 SQLite 数据库。"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db = tmp_path / "test.db"
    monkeypatch.setenv("DATABASE_PATH", str(db))

    from app import config, database, service
    importlib.reload(config)
    importlib.reload(database)
    importlib.reload(service)

    from fastapi.testclient import TestClient

    from app import main
    importlib.reload(main)

    with TestClient(main.app) as c:
        yield c
