from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

SPORTSMOLE_TABLE_URL = "https://www.sportsmole.co.uk/football/uruguayan-primera-division/table.html"
AFRISCORES_STATS_URL = "https://afriscores.com/en/league/uruguay/primera-division/770/stats"

STANDINGS_CSV = Path("data") / "standings_uruguay.csv"
CARDS_CSV = Path("data") / "discipline_uruguay_simple.csv"

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=40)


def _safe_int(value: str | int | float | None) -> int:
    try:
        if value is None:
            return 0
        return int(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return 0


async def fetch_html(url: str) -> str:
    """Descarga HTML de una URL. Devuelve cadena vacía en caso de error."""

    try:
        async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT) as session:
            async with session.get(url, headers={"User-Agent": USER_AGENT}) as resp:
                resp.raise_for_status()
                return await resp.text()
    except Exception as exc:  # pragma: no cover - defensivo frente a red externa
        print(f"Error al descargar {url}: {exc}")
        return ""


def _find_table_with_headers(soup: BeautifulSoup, expected_headers: Iterable[str]) -> Optional[BeautifulSoup]:
    expected = {h.lower() for h in expected_headers}
    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        if not headers:
            continue
        if expected.intersection(headers):
            return table
    return None


def _map_columns(headers: list[str], mapping: dict[str, list[str]]) -> dict[str, int]:
    header_norm = [h.strip().lower() for h in headers]
    indices: dict[str, int] = {}
    for target, aliases in mapping.items():
        for alias in aliases:
            if alias.lower() in header_norm:
                indices[target] = header_norm.index(alias.lower())
                break
    return indices


def _extract_row_values(cells: list[str], indices: dict[str, int]) -> dict[str, int | str]:
    row: dict[str, int | str] = {}
    for key, idx in indices.items():
        value = cells[idx] if idx < len(cells) else ""
        row[key] = value
    return row


async def fetch_standings_sportsmole() -> pd.DataFrame:
    """
    Descarga la tabla de posiciones desde SportsMole y la devuelve normalizada.
    Columnas: equipo, pj, pg, pe, pp, gf, gc, dg, pts
    """

    html = await fetch_html(SPORTSMOLE_TABLE_URL)
    if not html:
        raise RuntimeError("No se pudo descargar la tabla de SportsMole.")

    soup = BeautifulSoup(html, "lxml")
    table = _find_table_with_headers(soup, expected_headers={"team", "p", "pts", "w", "l"})
    if table is None:
        raise RuntimeError("No se encontró ninguna tabla de standings en SportsMole.")

    header_cells = [th.get_text(strip=True) for th in table.find_all("th")]
    mapping = {
        "equipo": ["team", "club"],
        "pj": ["p", "mp", "matches"],
        "pg": ["w", "wins"],
        "pe": ["d", "draws"],
        "pp": ["l", "losses"],
        "gf": ["f", "gf", "goals for"],
        "gc": ["a", "ga", "goals against"],
        "dg": ["gd", "goal difference", "diff"],
        "pts": ["pts", "points"],
    }
    indices = _map_columns(header_cells, mapping)
    if "equipo" not in indices or "pj" not in indices or "pts" not in indices:
        raise RuntimeError(
            "La tabla de SportsMole no contiene las columnas esperadas de equipo, partidos o puntos."
        )

    rows: list[dict[str, int | str]] = []
    for tr in table.find_all("tr"):
        cells = [c.get_text(strip=True) for c in tr.find_all("td")]
        if not cells:
            continue
        datos = _extract_row_values(cells, indices)
        nombre = str(datos.get("equipo", "")).strip()
        if not nombre:
            continue

        pj = _safe_int(datos.get("pj"))
        pg = _safe_int(datos.get("pg"))
        pe = _safe_int(datos.get("pe"))
        pp = _safe_int(datos.get("pp"))
        gf = _safe_int(datos.get("gf"))
        gc = _safe_int(datos.get("gc"))
        dg_val = datos.get("dg")
        dg = _safe_int(dg_val) if dg_val not in (None, "") else gf - gc
        pts = _safe_int(datos.get("pts"))

        rows.append(
            {
                "equipo": nombre,
                "pj": pj,
                "pg": pg,
                "pe": pe,
                "pp": pp,
                "gf": gf,
                "gc": gc,
                "dg": dg,
                "pts": pts,
            }
        )

    if not rows:
        raise RuntimeError("No se pudieron parsear filas de standings desde SportsMole.")

    return pd.DataFrame(rows)


def _find_heading(soup: BeautifulSoup, text: str) -> Optional[BeautifulSoup]:
    target = text.lower()
    for tag in soup.find_all(["h1", "h2", "h3", "h4"]):
        if target in tag.get_text(strip=True).lower():
            return tag
    return None


async def fetch_player_cards_afriscores() -> pd.DataFrame:
    """
    Descarga la sección de 'Yellow Cards' (y si existe, 'Red Cards') desde Afriscores
    y devuelve una tabla de jugadores con columnas: jugador, equipo, amarillas, rojas
    """

    html = await fetch_html(AFRISCORES_STATS_URL)
    if not html:
        raise RuntimeError("No se pudo descargar la página de Afriscores.")

    soup = BeautifulSoup(html, "lxml")
    heading = _find_heading(soup, "Yellow Cards")
    if heading is None:
        raise RuntimeError("No se encontró la sección de Yellow Cards en Afriscores.")

    table = heading.find_next("table")
    if table is None:
        raise RuntimeError("No se encontró la tabla de Yellow Cards en Afriscores.")

    header_cells = [th.get_text(strip=True) for th in table.find_all("th")]
    mapping = {
        "jugador": ["player", "jugador"],
        "equipo": ["team", "club", "equipo"],
        "amarillas": ["yellow cards", "yellow", "amarillas"],
        "rojas": ["red cards", "red", "rojas"],
    }
    indices = _map_columns(header_cells, mapping)
    if "jugador" not in indices or "equipo" not in indices or "amarillas" not in indices:
        raise RuntimeError(
            "La tabla de Afriscores no tiene columnas suficientes para tarjetas amarillas."
        )

    rows: list[dict[str, int | str]] = []
    for tr in table.find_all("tr"):
        cells = [c.get_text(strip=True) for c in tr.find_all("td")]
        if not cells:
            continue
        valores = _extract_row_values(cells, indices)
        jugador = str(valores.get("jugador", "")).strip()
        equipo = str(valores.get("equipo", "")).strip()
        if not jugador or not equipo:
            continue

        amarillas = _safe_int(valores.get("amarillas"))
        rojas = _safe_int(valores.get("rojas")) if "rojas" in indices else 0

        rows.append(
            {
                "jugador": jugador,
                "equipo": equipo,
                "amarillas": amarillas,
                "rojas": rojas,
            }
        )

    if not rows:
        raise RuntimeError("No se pudieron parsear filas de tarjetas en Afriscores.")

    return pd.DataFrame(rows)


def aggregate_cards_by_team(df_players: pd.DataFrame) -> pd.DataFrame:
    """
    A partir del DF de jugadores, genera una tabla por equipo con:
      equipo, amarillas, rojas, tarjetas_totales
    """

    if df_players.empty:
        return pd.DataFrame(columns=["equipo", "amarillas", "rojas", "tarjetas_totales"])

    grouped = df_players.groupby("equipo", dropna=True)[["amarillas", "rojas"]].sum().reset_index()
    grouped["tarjetas_totales"] = grouped["amarillas"] + grouped["rojas"]
    return grouped


def save_standings(df: pd.DataFrame, path: Path = STANDINGS_CSV) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")


def save_cards(df: pd.DataFrame, path: Path = CARDS_CSV) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")


def load_cards(path: Path = CARDS_CSV) -> list[dict]:
    if not path.exists():
        return []
    return pd.read_csv(path).to_dict(orient="records")
