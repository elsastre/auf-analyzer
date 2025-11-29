from fastapi.testclient import TestClient

from api import app
from auf_analyzer.storage import compute_table, seed_if_needed


def test_seed_creates_db(tmp_path, monkeypatch):
    db_path = tmp_path / "auf.db"
    monkeypatch.setattr("auf_analyzer.storage.db.DB_PATH", db_path)
    seed_if_needed(db_path)
    assert db_path.exists()
    table = compute_table(2024, "apertura")
    assert table["rows"]


def test_meta_endpoint_has_seasons_and_stages(tmp_path, monkeypatch):
    db_path = tmp_path / "auf.db"
    monkeypatch.setattr("auf_analyzer.storage.db.DB_PATH", db_path)
    seed_if_needed(db_path)
    client = TestClient(app)
    resp = client.get("/meta")
    assert resp.status_code == 200
    body = resp.json()
    assert 2024 in body["seasons"]
    assert "apertura" in body["stages"]


def test_tables_endpoint_returns_rows(tmp_path, monkeypatch):
    db_path = tmp_path / "auf.db"
    monkeypatch.setattr("auf_analyzer.storage.db.DB_PATH", db_path)
    seed_if_needed(db_path)
    client = TestClient(app)
    resp = client.get("/tables?season=2024&stage=apertura")
    assert resp.status_code == 200
    body = resp.json()
    assert body["rows"]
    assert body["stage"] == "apertura"
