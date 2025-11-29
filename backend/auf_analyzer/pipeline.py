"""Pipelines asincrónicos para refrescar datos desde distintas fuentes."""

from __future__ import annotations

import asyncio
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import aiohttp
from bs4 import BeautifulSoup

from .services import CSV_STANDINGS
from .simple_scraper import (
    CARDS_CSV,
    fetch_player_cards_afriscores,
    fetch_standings_sportsmole,
    aggregate_cards_by_team,
    save_cards,
    save_standings,
)
from .tsdl_scraper import (
    CARDS_TSDL_CSV,
    STANDINGS_TSDL_CSV,
    fetch_cards_table,
    fetch_extra_stats,
    fetch_league_table,
)
from .worldfootball import (
    DISCIPLINE_WORLD_CSV,
    aggregate_discipline_by_team,
    fetch_standings_worldfootball,
    fetch_team_appearances_worldfootball,
    save_discipline_to_csv,
    save_standings_to_csv,
)

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=40)
DATA_DIR = CSV_STANDINGS.parent
SUMMARY_FILE = DATA_DIR / "pipeline_summary.json"


@dataclass(frozen=True)
class StageSource:
    """Describe un torneo o fase específica dentro de la AUF."""

    slug: str
    label: str
    season: str
    url: str
    is_master: bool = False


STAGE_SOURCES: tuple[StageSource, ...] = (
    StageSource(
        slug="primera_division_global",
        label="Tabla general Primera División Uruguay",
        season="2024-2025",
        url="https://fbref.com/en/comps/45/table/Primera-Division-Uruguay-Stats",
        is_master=True,
    ),
    StageSource(
        slug="apertura_2024",
        label="Torneo Apertura 2024",
        season="2024",
        url="https://fbref.com/en/comps/45/Apertura-2024-Stats",
    ),
    StageSource(
        slug="intermedio_2024",
        label="Torneo Intermedio 2024",
        season="2024",
        url="https://fbref.com/en/comps/45/Intermedio-2024-Stats",
    ),
    StageSource(
        slug="clausura_2024",
        label="Torneo Clausura 2024",
        season="2024",
        url="https://fbref.com/en/comps/45/Clausura-2024-Stats",
    ),
    StageSource(
        slug="apertura_2025",
        label="Torneo Apertura 2025",
        season="2025",
        url="https://fbref.com/en/comps/45/Apertura-2025-Stats",
    ),
    StageSource(
        slug="intermedio_2025",
        label="Torneo Intermedio 2025",
        season="2025",
        url="https://fbref.com/en/comps/45/Intermedio-2025-Stats",
    ),
    StageSource(
        slug="clausura_2025",
        label="Torneo Clausura 2025",
        season="2025",
        url="https://fbref.com/en/comps/45/Clausura-2025-Stats",
    ),
)


@dataclass
class StageResult:
    stage: StageSource
    rows: list[list[str]]
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and len(self.rows) > 1


def _build_rows_from_html(html: str) -> list[list[str]]:
    soup = BeautifulSoup(html, "html5lib")
    table = soup.find("table")
    if table is None:
        return []

    headers = [th.get_text(strip=True) for th in table.select("thead tr th")]
    rows: list[list[str]] = []
    if headers:
        rows.append(headers)

    for tr in table.select("tbody tr"):
        cells = [c.get_text(strip=True) for c in tr.select("th,td")]
        if cells:
            rows.append(cells)

    return rows


async def _download_stage(session: aiohttp.ClientSession, stage: StageSource) -> StageResult:
    try:
        async with session.get(stage.url, headers={"User-Agent": USER_AGENT}) as resp:
            resp.raise_for_status()
            html = await resp.text()
    except Exception as exc:  # pragma: no cover - red de terceros
        return StageResult(stage=stage, rows=[], error=str(exc))

    rows = _build_rows_from_html(html)
    if not rows:
        return StageResult(stage=stage, rows=[], error="No se encontró la tabla en la página")

    return StageResult(stage=stage, rows=rows)


async def fetch_all_stages(
    stages: Sequence[StageSource] = STAGE_SOURCES,
) -> list[StageResult]:
    """Descarga todas las tablas de manera concurrente usando asyncio + aiohttp."""

    connector = aiohttp.TCPConnector(limit=5)
    async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT, connector=connector) as session:
        tasks = [_download_stage(session, stage) for stage in stages]
        return await asyncio.gather(*tasks)


def _stage_csv_path(stage: StageSource) -> Path:
    return DATA_DIR / f"standings_{stage.slug}.csv"


def stage_csv_path_from_slug(slug: str) -> Path:
    for stage in STAGE_SOURCES:
        if stage.slug == slug:
            return _stage_csv_path(stage)
    raise KeyError(f"No existe ninguna etapa registrada con slug '{slug}'.")


def persist_stage_result(result: StageResult) -> Path | None:
    if not result.ok:
        return None

    DATA_DIR.mkdir(exist_ok=True)
    csv_path = _stage_csv_path(result.stage)
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(result.rows)
    return csv_path


def _select_master_result(results: Iterable[StageResult]) -> StageResult | None:
    preferred = [r for r in results if r.stage.is_master and r.ok]
    if preferred:
        return preferred[0]

    ok_results = [r for r in results if r.ok]
    if ok_results:
        return ok_results[0]

    return None


def _persist_summary(results: Sequence[StageResult]) -> dict:
    summary = {
        "stages": [
            {
                "slug": r.stage.slug,
                "label": r.stage.label,
                "season": r.stage.season,
                "ok": r.ok,
                "rows": len(r.rows),
                "error": r.error,
                "csv_path": str(_stage_csv_path(r.stage)) if r.ok else None,
            }
            for r in results
        ],
    }

    DATA_DIR.mkdir(exist_ok=True)
    SUMMARY_FILE.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


async def run_pipeline() -> dict:
    """Ejecuta el pipeline asincrónico completo y devuelve un resumen."""

    results = await fetch_all_stages()
    for result in results:
        persist_stage_result(result)

    master = _select_master_result(results)
    if master is None:
        raise RuntimeError("No se pudo descargar ninguna tabla de FBref.")

    # Sincronizamos el archivo principal usado por la API / servicios.
    csv_path = _stage_csv_path(master.stage)
    if csv_path.exists():
        CSV_STANDINGS.parent.mkdir(exist_ok=True)
        CSV_STANDINGS.write_text(csv_path.read_text(encoding="utf-8"), encoding="utf-8")

    return _persist_summary(results)


async def run_worldfootball_pipeline(season: int = 2025) -> dict:
    """
    Descarga standings y disciplina desde worldfootball.net y guarda los CSV.

    Genera:
      - data/standings_uruguay.csv (compatible con los endpoints existentes)
      - data/discipline_uruguay_worldfootball.csv
    """

    standings = await fetch_standings_worldfootball(season)
    if not standings:
        raise RuntimeError("No se pudieron obtener standings desde worldfootball.net")

    save_standings_to_csv(standings, CSV_STANDINGS)

    partidos_por_equipo = {row.get("equipo"): row.get("pj", 0) for row in standings if row.get("equipo")}

    appearances = await fetch_team_appearances_worldfootball(season, equipos=list(partidos_por_equipo.keys()))
    discipline_rows = aggregate_discipline_by_team(appearances, partidos_por_equipo=partidos_por_equipo)
    save_discipline_to_csv(discipline_rows, path=DISCIPLINE_WORLD_CSV)

    return {
        "season": season,
        "num_equipos": len(standings),
        "num_jugadores": len(appearances),
        "generados": [str(CSV_STANDINGS), str(DISCIPLINE_WORLD_CSV)],
        "source": "worldfootball.net",
    }


async def run_simple_pipeline() -> dict:
    """
    Pipeline principal basada en SportsMole + Afriscores.

    - Descarga standings.
    - Descarga tarjetas por jugador, agrega por equipo.
    - Guarda CSV en backend/data.
    - Devuelve un resumen para el endpoint /standings/refresh.
    """

    df_standings, df_players = await asyncio.gather(
        fetch_standings_sportsmole(), fetch_player_cards_afriscores()
    )

    df_cards = aggregate_cards_by_team(df_players)

    save_standings(df_standings, path=CSV_STANDINGS)
    save_cards(df_cards, path=CARDS_CSV)

    return {
        "source": "sportsmole+afriscores",
        "num_equipos": len(df_standings),
        "num_equipos_disciplina": len(df_cards),
        "columns_standings": list(df_standings.columns),
        "columns_cards": list(df_cards.columns),
    }


async def run_tsdl_pipeline(include_extra: bool = False) -> dict:
    """
    Descarga standings y estadísticas de tarjetas desde TheStatsDontLie,
    genera CSV en backend/data y devuelve un resumen.
    """

    df_standings, df_cards = await asyncio.gather(
        fetch_league_table(), fetch_cards_table()
    )

    save_standings(df_standings, path=STANDINGS_TSDL_CSV)
    save_cards(df_cards, path=CARDS_TSDL_CSV)

    # Sincronizamos el archivo principal esperado por el resto del backend.
    save_standings(df_standings, path=CSV_STANDINGS)

    extra_stats: dict[str, object] = {}
    if include_extra:
        extra_stats_raw = await fetch_extra_stats(
            "btts", "overs_unders", "corners", "avg_team_goals"
        )
        extra_stats = {key: list(df.columns) for key, df in extra_stats_raw.items()}
        for key, df in extra_stats_raw.items():
            extra_path = Path("data") / f"{key}_uruguay_tsdl.csv"
            extra_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(extra_path, index=False, encoding="utf-8")

    return {
        "source": "thestatsdontlie.com",
        "num_equipos": len(df_standings),
        "columns_standings": list(df_standings.columns),
        "columns_cards": list(df_cards.columns),
        "extra_stats": list(extra_stats.keys()) if include_extra else [],
    }


def available_stage_slugs() -> list[str]:
    return [stage.slug for stage in STAGE_SOURCES]
