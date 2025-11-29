from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

FORM_GUIDE_URL = "https://www.sportsmole.co.uk/football/uruguayan-primera-division/2024/form-guide.html"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
}
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "data"))
OUTPUT_JSON = OUTPUT_DIR / "form_guide_uruguay_2024.json"
OUTPUT_CSV = OUTPUT_DIR / "form_guide_uruguay_2024.csv"


def fetch(url: str) -> str:
    """Descarga HTML con headers de navegador y timeout razonable."""

    response = requests.get(url, headers=HEADERS, timeout=20)
    response.raise_for_status()
    return response.text


def table_to_records(table: BeautifulSoup) -> list[dict[str, Any]]:
    """Convierte una tabla HTML a una lista de dicts usando headers como claves."""

    headers: list[str] = []
    thead = table.find("thead")
    if thead:
        headers = [th.get_text(strip=True) for th in thead.find_all("th")]
    else:
        first_row = table.find("tr")
        if first_row:
            headers = [td.get_text(strip=True) for td in first_row.find_all(["th", "td"])]

    rows: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if not cells:
            continue
        if thead and tr.find_parent("thead"):
            continue
        texts = [c.get_text(" ", strip=True) for c in cells]
        if headers and len(texts) == len(headers):
            row: dict[str, Any] = dict(zip(headers, texts))
        else:
            row = {f"col_{i}": texts[i] for i in range(len(texts))}
        links = [a.get("href") for a in tr.find_all("a", href=True)]
        if links:
            row["_links"] = links
        rows.append(row)
    return rows


def guess_form_guide(soup: BeautifulSoup) -> list[dict[str, Any]]:
    """Busca tablas o secciones relacionadas al form guide y devuelve registros."""

    records: list[dict[str, Any]] = []
    tables = soup.find_all("table")
    for table in tables:
        recs = table_to_records(table)
        if recs:
            records.append({"source": "table", "records": recs})

    candidates = soup.find_all(
        lambda tag: tag.name in ("div", "section", "article")
        and (
            (tag.get("class") and any("form" in c.lower() for c in tag.get("class")))
            or (tag.get("id") and "form" in tag.get("id").lower())
        )
    )
    for candidate in candidates:
        items: list[str] = []
        for row in candidate.find_all(["li", "p", "div"]):
            text = row.get_text(" ", strip=True)
            if text:
                items.append(text)
        if items:
            records.append({"source": "form-section", "records": [{"text": t} for t in items]})

    return records


def scrape_form_guide(
    url: str = FORM_GUIDE_URL, save_files: bool = False, output_dir: str | Path | None = None
) -> dict[str, Any]:
    """
    Descarga y parsea el form guide de SportsMole.

    Devuelve un dict con:
    {
      "url": ...,
      "title": ...,
      "description": ...,
      "scraped_at": ...,
      "guides": [ { "source": ..., "records": [...] }, ... ]
    }
    Si save_files=True, guarda el JSON en data/ y (opcionalmente) un CSV simple.
    """

    output = Path(output_dir) if output_dir else OUTPUT_DIR
    output.mkdir(parents=True, exist_ok=True)

    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")
    page_title = soup.title.string.strip() if soup.title and soup.title.string else url
    md = soup.find("meta", attrs={"name": "description"}) or soup.find(
        "meta", attrs={"property": "og:description"}
    )
    meta_desc = md["content"].strip() if md and md.get("content") else ""
    data: dict[str, Any] = {
        "url": url,
        "title": page_title,
        "description": meta_desc,
        "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    data["guides"] = guess_form_guide(soup)

    if save_files:
        output_json = output / OUTPUT_JSON.name
        output_json.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        # CSV opcional: depende del formato de records; aquí se mantiene placeholder
        # para evitar romper flujos existentes.

    return data


__all__ = [
    "FORM_GUIDE_URL",
    "HEADERS",
    "OUTPUT_DIR",
    "OUTPUT_JSON",
    "OUTPUT_CSV",
    "fetch",
    "table_to_records",
    "guess_form_guide",
    "scrape_form_guide",
]
