"""Scraper basado en TheStatsDontLie para standings y estadísticas de tarjetas.

Provee funciones asincrónicas para obtener standings, tarjetas y otras
estadísticas de la Primera División de Uruguay.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Literal

import aiohttp
import pandas as pd

BASE_LEAGUE_URL = "https://www.thestatsdontlie.com/football/n-s-america/uruguay/primera-division/"

STAT_SLUGS = {
    "tabla_liga": "",  # tabla principal
    "btts": "btts/",
    "overs_unders": "overs-unders/",
    "btts_y_resultado": "btts-and-match-result/",
    "win_draw_loss_pct": "win-draw-loss-percentage/",
    "xg": "xg-stats/",
    "corners": "corners/",
    "cards": "cards/",
    "shots": "shots/",
    "fouls": "fouls/",
    "correct_scores": "correct-scores/",
    "half_time_full_time": "half-time-full-time/",
    "scored_both_halves": "scored-both-halves/",
    "won_both_halves": "won-both-halves/",
    "first_second_half_goals": "1st-2nd-half-goals/",
    "rescued_points": "rescued-points/",
    "clean_sheets_failed_to_score": "clean-sheets-failed-to-score/",
    "won_to_nil": "won-to-nil/",
    "winning_losing_margin": "winning-losing-margin/",
    "scored_first": "scored-first/",
    "scored_two_or_more": "scored-2-or-more/",
    "avg_first_goal_time": "average-1st-goal-time/",
    "avg_team_goals": "average-team-goals/",
    "half_time_stats": "half-time-stats/",
    "early_goals": "early-goals/",
    "late_goals": "late-goals/",
}

STANDINGS_TSDL_CSV = Path("data") / "standings_uruguay_tsdl.csv"
CARDS_TSDL_CSV = Path("data") / "cards_uruguay_tsdl.csv"


async def fetch_html(session: aiohttp.ClientSession, url: str) -> str:
    async with session.get(url) as resp:
        resp.raise_for_status()
        return await resp.text()


def _normalize_columns(df: pd.DataFrame, mapping: dict[str, list[str]], *, default_prefix: str = "col") -> pd.DataFrame:
    df = df.copy()
    normalized = {}
    for col in df.columns:
        col_str = str(col).strip().lower()
        match = None
        for target, aliases in mapping.items():
            if col_str in [a.lower() for a in aliases]:
                match = target
                break
        if match is None:
            match = f"{default_prefix}_{col_str}"
        normalized[col] = match
    df = df.rename(columns=normalized)
    return df


def _parse_main_table(html: str) -> pd.DataFrame:
    tables = pd.read_html(html)
    if not tables:
        raise ValueError("No se encontró ninguna tabla en la página descargada")
    return tables[0]


async def fetch_league_table() -> pd.DataFrame:
    """Descarga y normaliza la tabla principal de posiciones."""

    async with aiohttp.ClientSession() as session:
        html = await fetch_html(session, BASE_LEAGUE_URL)

    df_raw = _parse_main_table(html)

    mapping = {
        "equipo": ["team", "squad", "equipo", "club"],
        "pj": ["mp", "p", "pld", "pj"],
        "pg": ["w", "wins", "ganados"],
        "pe": ["d", "draws", "empates"],
        "pp": ["l", "losses", "perdidos"],
        "gf": ["gf", "f", "goals for", "goles a favor"],
        "gc": ["ga", "a", "goals against", "goles en contra"],
        "dg": ["gd", "diff", "+/-", "diferencia"],
        "pts": ["pts", "points", "puntos"],
    }
    df_norm = _normalize_columns(df_raw, mapping)

    columnas_finales = ["equipo", "pj", "pg", "pe", "pp", "gf", "gc", "dg", "pts"]
    df_out = pd.DataFrame()
    for col in columnas_finales:
        if col == "equipo":
            df_out[col] = df_norm.filter(regex=r"^equipo$").iloc[:, 0]
        else:
            candidatos = [c for c in df_norm.columns if c.startswith(col)]
            if candidatos:
                serie = pd.to_numeric(df_norm[candidatos[0]], errors="coerce").fillna(0).astype(int)
            else:
                serie = pd.Series([0] * len(df_norm))
            df_out[col] = serie

    return df_out


async def fetch_cards_table() -> pd.DataFrame:
    """Descarga y normaliza la tabla de tarjetas por equipo."""

    url = BASE_LEAGUE_URL + STAT_SLUGS["cards"]
    async with aiohttp.ClientSession() as session:
        html = await fetch_html(session, url)

    df_raw = _parse_main_table(html)
    mapping = {
        "equipo": ["team", "squad", "equipo", "club"],
        "partidos": ["matches", "mp", "pj", "games"],
        "amarillas": ["yellow", "cards y", "yellow cards", "amarillas"],
        "rojas": ["red", "red cards", "rojas"],
        "tarjetas_totales": ["cards", "total cards", "tarjetas"],
        "tarjetas_pp": ["cards per game", "cards/game", "tarjetas/partido"],
    }
    df_norm = _normalize_columns(df_raw, mapping, default_prefix="stat")

    df_out = pd.DataFrame()
    df_out["equipo"] = df_norm.filter(regex=r"^equipo$").iloc[:, 0]
    for col, fallbacks in (
        ("partidos", ["partidos", "matches", "mp", "pj", "games"]),
        ("amarillas", ["amarillas", "yellow", "cards y", "yellow cards"]),
        ("rojas", ["rojas", "red", "red cards"]),
        ("tarjetas_totales", ["tarjetas_totales", "cards", "total cards"]),
        ("tarjetas_pp", ["tarjetas_pp", "cards per game", "cards/game", "tarjetas/partido"]),
    ):
        candidatos = [c for c in df_norm.columns if any(c.startswith(fb) for fb in fallbacks)]
        if candidatos:
            serie = pd.to_numeric(df_norm[candidatos[0]], errors="coerce")
        else:
            serie = pd.Series([0] * len(df_norm))
        df_out[col] = serie.fillna(0)

    if "tarjetas_pp" not in df_out.columns:
        df_out["tarjetas_pp"] = 0
    if "partidos" in df_out.columns and "tarjetas_totales" in df_out.columns:
        partidos = df_out["partidos"].replace(0, pd.NA)
        with pd.option_context("mode.use_inf_as_na", True):
            df_out["tarjetas_pp"] = (
                df_out["tarjetas_totales"] / partidos
            ).fillna(df_out["tarjetas_pp"]).round(3)
    df_out["tarjetas_por_partido"] = df_out["tarjetas_pp"]

    return df_out


async def fetch_extra_stats(*slugs: Literal[*tuple(STAT_SLUGS.keys())]) -> dict[str, pd.DataFrame]:
    extra: dict[str, pd.DataFrame] = {}
    if not slugs:
        return extra

    valid_slugs: list[str] = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for slug in slugs:
            if slug not in STAT_SLUGS:
                continue
            valid_slugs.append(slug)
            url = BASE_LEAGUE_URL + STAT_SLUGS[slug]
            tasks.append(asyncio.create_task(fetch_html(session, url)))
        html_results = await asyncio.gather(*tasks)

    for slug, html in zip(valid_slugs, html_results):
        df_raw = _parse_main_table(html)
        df_raw.columns = [str(c).strip().lower() for c in df_raw.columns]
        if "team" in df_raw.columns:
            df_raw = df_raw.rename(columns={"team": "equipo"})
        elif "squad" in df_raw.columns:
            df_raw = df_raw.rename(columns={"squad": "equipo"})
        extra[slug] = df_raw

    return extra


def save_standings(df: pd.DataFrame, path: Path = STANDINGS_TSDL_CSV) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")


def save_cards(df: pd.DataFrame, path: Path = CARDS_TSDL_CSV) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")


def load_cards(path: Path = CARDS_TSDL_CSV) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"No se encontró el CSV de tarjetas en {path}")
    df = pd.read_csv(path)
    return df.to_dict(orient="records")
