import importlib.util
from pathlib import Path

import importlib.util
from pathlib import Path

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient
from auf_analyzer import services


API_PATH = Path(__file__).resolve().parents[1] / "api.py"


def _load_api_module():
    spec = importlib.util.spec_from_file_location("backend_api", API_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_load_rows_uses_sample_when_main_missing(monkeypatch, tmp_path):
    csv_main = tmp_path / "standings_uruguay.csv"
    sample = tmp_path / "standings_uruguay_sample.csv"
    sample.write_text(
        "Squad,MP,W,D,L,GF,GA,Pts\nCerro,1,0,0,1,0,1,0\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(services, "CSV_STANDINGS", csv_main)
    monkeypatch.setattr(services, "SAMPLE_STANDINGS_CSV", sample)

    headers, rows = services._load_rows()

    assert headers == ["Squad", "MP", "W", "D", "L", "GF", "GA", "Pts"]
    assert len(rows) == 1
    assert rows[0][0] == "Cerro"


def test_api_list_equipos_without_any_csv(monkeypatch, tmp_path):
    missing = tmp_path / "does_not_exist.csv"
    monkeypatch.setattr(services, "CSV_STANDINGS", missing)
    monkeypatch.setattr(services, "SAMPLE_STANDINGS_CSV", missing)

    backend_api = _load_api_module()
    client = TestClient(backend_api.app)
    response = client.get("/torneo/equipos")

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 0
    assert payload["equipos"] == []
    assert "message" in payload
