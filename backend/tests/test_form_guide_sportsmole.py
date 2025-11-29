from __future__ import annotations

import json

from fastapi.testclient import TestClient

import api
from auf_analyzer import form_guide_sportsmole as fg


def test_scrape_form_guide_parses_html(monkeypatch, tmp_path):
    sample_html = """
    <html>
      <head>
        <title>Sample Form Guide</title>
        <meta name="description" content="Sample description" />
      </head>
      <body>
        <table>
          <thead>
            <tr><th>Team</th><th>Form</th></tr>
          </thead>
          <tbody>
            <tr><td>Team A</td><td>WWW</td></tr>
            <tr><td>Team B</td><td>LLL</td></tr>
          </tbody>
        </table>
        <div id="form-guide">
          <p>Extra form info</p>
        </div>
      </body>
    </html>
    """

    monkeypatch.setattr(fg, "fetch", lambda url: sample_html)

    data = fg.scrape_form_guide(url="http://test", save_files=False, output_dir=tmp_path)

    assert set(["url", "title", "description", "scraped_at", "guides"]).issubset(data.keys())
    assert isinstance(data["guides"], list)
    assert any("source" in item and "records" in item for item in data["guides"])


def test_get_form_guide_endpoint(monkeypatch, tmp_path):
    sample_data = {
        "url": "http://local",
        "title": "Local",
        "description": "",
        "scraped_at": "now",
        "guides": [{"source": "table", "records": [{"Team": "X", "Form": "WWW"}]}],
    }
    output_json = tmp_path / "form_guide_uruguay_2024.json"
    output_json.write_text(json.dumps(sample_data), encoding="utf-8")

    monkeypatch.setattr(api, "OUTPUT_JSON", output_json)
    monkeypatch.setattr(api, "scrape_form_guide", lambda **kwargs: sample_data)

    client = TestClient(api.app)
    response = client.get("/stats/form-guide")

    assert response.status_code == 200
    payload = response.json()
    assert "guides" in payload
    assert payload["guides"]
