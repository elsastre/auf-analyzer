from __future__ import annotations

import asyncio
import csv
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import aiohttp
from bs4 import BeautifulSoup

from .services import TEAM_META

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=40)
STANDINGS_URL_TEMPLATE = "https://www.worldfootball.net/competition/uru-primera-division-{season}-{stage}/"
APPEARANCES_URL_TEMPLATE = (
    "https://www.worldfootball.net/team_performance/{team_slug}/uru-primera-division-{season}-{stage}/"
)
DISCIPLINE_WORLD_CSV = Path("data") / "discipline_uruguay_worldfootball.csv"

# Mapeo manual de slugs de equipos en worldfootball.net
TEAM_SLUGS = {
    "Nacional": "nacional-montevideo",
    "Peñarol": "penarol-montevideo",
    "Liverpool": "liverpool-montevideo",
    "Liverpool FC": "liverpool-montevideo",
    "Danubio": "danubio-fc",
    "Defensor": "defensor-sporting",
    "Defensor Sporting": "defensor-sporting",
    "River Plate": "river-plate-montevideo",
    "River Plate Montevideo": "river-plate-montevideo",
    "Racing": "racing-club-de-montevideo",
    "Cerro": "club-atletico-cerro",
    "Cerro Largo": "cerro-largo-fc",
    "Torque": "montevideo-city-torque",
    "Montevideo City Torque": "montevideo-city-torque",
    "Boston River": "boston-river",
    "Plaza Colonia": "plaza-colonia",
    "Wanderers": "montevideo-wanderers",
    "Montevideo Wanderers": "montevideo-wanderers",
    "Miramar Misiones": "miramar-misiones",
    "Juventud de Las Piedras": "juventud-de-las-piedras",
    "Progreso": "club-atletico-progreso",
    "Fenix": "centro-atletico-fenix",
    "Fénix": "centro-atletico-fenix",
}

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StandingsStage:
    stage: str
    label: str


STAGE_ORDER: tuple[StandingsStage, ...] = (
    StandingsStage(stage="apertura", label="Apertura"),
    StandingsStage(stage="intermedio", label="Intermedio"),
    StandingsStage(stage="clausura", label="Clausura"),
)


def _normalize_team_name(nombre: str) -> str:
    nombre = nombre.strip()
    if not nombre:
        return ""

    for canonical in TEAM_META.keys():
        if nombre.lower() == canonical.lower():
            return canonical
    for canonical in TEAM_META.keys():
        if nombre.lower() in canonical.lower() or canonical.lower() in nombre.lower():
            return canonical
    return nombre


def _slugify_team(nombre: str) -> str:
    if nombre in TEAM_SLUGS:
        return TEAM_SLUGS[nombre]

    # Fallback básico: minúsculas, reemplazo de espacios y eliminación de acentos simples
    nombre_norm = nombre.lower()
    nombre_norm = nombre_norm.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
    nombre_norm = re.sub(r"[^a-z0-9]+", "-", nombre_norm).strip("-")
    return nombre_norm


async def _fetch_html(session: aiohttp.ClientSession, url: str) -> str | None:
    try:
        async with session.get(url, headers={"User-Agent": USER_AGENT}) as resp:
            resp.raise_for_status()
            return await resp.text()
    except asyncio.TimeoutError:
        logger.warning("Timeout al descargar %s", url)
    except aiohttp.ClientError as exc:
        logger.warning("No se pudo descargar %s: %s", url, exc)
    except Exception as exc:  # pragma: no cover - defensa adicional
        logger.warning("Error inesperado al descargar %s: %s", url, exc)
    return None


def _parse_standings_table(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html5lib")
    table = soup.find("table", class_="std_table") or soup.find("table")
    if table is None:
        return []

    headers = [th.get_text(strip=True).lower() for th in table.select("thead tr th")]
    body_rows = table.select("tbody tr")
    results: list[dict] = []

    def find_col(posibles: Iterable[str]) -> int | None:
        for name in posibles:
            if name.lower() in headers:
                return headers.index(name.lower())
        return None

    col_team = find_col(["team", "club", "equipo"])
    col_mp = find_col(["m", "mp", "matches"])
    col_w = find_col(["w"])
    col_d = find_col(["d"])
    col_l = find_col(["l"])
    col_score = find_col(["score", "goals"])
    col_diff = find_col(["diff", "+/-"])
    col_pts = find_col(["pts", "points"])

    if col_team is None:
        return []

    for tr in body_rows:
        cells = [c.get_text(strip=True) for c in tr.select("th,td")]
        if len(cells) <= col_team:
            continue

        team = _normalize_team_name(cells[col_team])
        if not team:
            continue

        def _safe_idx(col: int | None) -> str:
            if col is None:
                return "0"
            if col >= len(cells):
                return "0"
            return cells[col]

        mp = int(_safe_idx(col_mp) or 0)
        w = int(_safe_idx(col_w) or 0)
        d = int(_safe_idx(col_d) or 0)
        l = int(_safe_idx(col_l) or 0)

        gf = gc = 0
        score_raw = _safe_idx(col_score)
        if ":" in score_raw:
            try:
                gf, gc = [int(x) for x in score_raw.split(":", 1)]
            except ValueError:
                gf = gc = 0
        diff_raw = _safe_idx(col_diff)
        try:
            diff = int(diff_raw)
        except ValueError:
            diff = gf - gc

        try:
            pts = int(_safe_idx(col_pts) or 0)
        except ValueError:
            pts = 0

        results.append(
            {
                "equipo": team,
                "pj": mp,
                "pg": w,
                "pe": d,
                "pp": l,
                "gf": gf,
                "gc": gc,
                "dg": diff if diff is not None else gf - gc,
                "pts": pts,
            }
        )

    return results


async def fetch_standings_worldfootball(season: int) -> list[dict]:
    connector = aiohttp.TCPConnector(limit=4)
    async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT, connector=connector) as session:
        aggregated: dict[str, dict] = {}
        for stage in STAGE_ORDER:
            url = STANDINGS_URL_TEMPLATE.format(season=season, stage=stage.stage)
            html = await _fetch_html(session, url)
            if not html:
                continue

            rows = _parse_standings_table(html)
            if not rows:
                logger.warning("No se pudieron parsear standings para %s (%s)", stage.label, url)
                continue

            for row in rows:
                equipo = row["equipo"]
                if equipo not in aggregated:
                    aggregated[equipo] = row.copy()
                else:
                    agg = aggregated[equipo]
                    agg["pj"] += row.get("pj", 0)
                    agg["pg"] += row.get("pg", 0)
                    agg["pe"] += row.get("pe", 0)
                    agg["pp"] += row.get("pp", 0)
                    agg["gf"] += row.get("gf", 0)
                    agg["gc"] += row.get("gc", 0)
                    agg["dg"] = agg.get("gf", 0) - agg.get("gc", 0)
                    agg["pts"] += row.get("pts", 0)

            await asyncio.sleep(0.3)

    return list(aggregated.values())


def _parse_int(value: str | None) -> int:
    if value is None:
        return 0
    try:
        return int(re.sub(r"[^0-9]", "", value))
    except (TypeError, ValueError):
        return 0


def _parse_appearances_table(html: str, equipo: str) -> list[dict]:
    soup = BeautifulSoup(html, "html5lib")
    table = soup.find("table", class_="std_table") or soup.find("table")
    if table is None:
        return []

    headers = [th.get_text(strip=True).lower() for th in table.select("thead tr th")]
    body_rows = table.select("tbody tr")

    def find_col(options: Sequence[str]) -> int | None:
        for name in options:
            if name.lower() in headers:
                return headers.index(name.lower())
        return None

    col_player = find_col(["player", "jugador", "name"])
    col_apps = find_col(["appearances", "apps", "games", "m"])
    col_minutes = find_col(["minutes", "mins"])
    col_yellow = find_col(["yellow cards", "yellow", "amarillas"])
    col_second_yellow = find_col(["second yellow", "second yellow cards", "2nd yellow"])
    col_red = find_col(["red cards", "red", "rojas"])
    col_sub_in = find_col(["substitutions on", "sub on", "sub in"])
    col_sub_out = find_col(["substitutions off", "sub off", "sub out"])

    if col_player is None:
        return []

    registros: list[dict] = []
    for tr in body_rows:
        cells = [c.get_text(strip=True) for c in tr.select("th,td")]
        if len(cells) <= col_player:
            continue
        jugador = cells[col_player].strip()
        if not jugador:
            continue

        registros.append(
            {
                "equipo": equipo,
                "jugador": jugador,
                "partidos": _parse_int(cells[col_apps]) if col_apps is not None and col_apps < len(cells) else 0,
                "minutos": _parse_int(cells[col_minutes]) if col_minutes is not None and col_minutes < len(cells) else 0,
                "amarillas": _parse_int(cells[col_yellow]) if col_yellow is not None and col_yellow < len(cells) else 0,
                "segundas_amarillas": _parse_int(cells[col_second_yellow]) if col_second_yellow is not None and col_second_yellow < len(cells) else 0,
                "rojas": _parse_int(cells[col_red]) if col_red is not None and col_red < len(cells) else 0,
                "cambios_entrada": _parse_int(cells[col_sub_in]) if col_sub_in is not None and col_sub_in < len(cells) else 0,
                "cambios_salida": _parse_int(cells[col_sub_out]) if col_sub_out is not None and col_sub_out < len(cells) else 0,
            }
        )

    return registros


async def fetch_team_appearances_worldfootball(
    season: int,
    equipos: Sequence[str] | None = None,
) -> list[dict]:
    connector = aiohttp.TCPConnector(limit=4)
    async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT, connector=connector) as session:
        registros: list[dict] = []
        equipos_objetivo = list(equipos) if equipos else []

        for equipo in equipos_objetivo:
            slug = _slugify_team(equipo)
            for stage in STAGE_ORDER:
                url = APPEARANCES_URL_TEMPLATE.format(team_slug=slug, season=season, stage=stage.stage)
                html = await _fetch_html(session, url)
                if not html:
                    continue

                rows = _parse_appearances_table(html, equipo)
                if rows:
                    registros.extend(rows)
                else:
                    logger.warning("No se pudieron parsear appearances para %s (%s)", equipo, url)

                await asyncio.sleep(0.3)

    return registros


def aggregate_discipline_by_team(rows: Sequence[dict], partidos_por_equipo: dict[str, int] | None = None) -> list[dict]:
    aggregate: dict[str, dict] = {}
    partidos_por_equipo = partidos_por_equipo or {}

    for row in rows:
        equipo = row.get("equipo")
        if not equipo:
            continue

        entry = aggregate.setdefault(
            equipo,
            {
                "equipo": equipo,
                "amarillas": 0,
                "rojas": 0,
                "tarjetas_por_partido": 0.0,
                "cambios_totales": 0,
                "cambios_por_partido": 0.0,
                "partidos": partidos_por_equipo.get(equipo, 0),
                "fuente": "worldfootball",
            },
        )

        entry["amarillas"] += int(row.get("amarillas", 0)) + int(row.get("segundas_amarillas", 0))
        entry["rojas"] += int(row.get("rojas", 0))
        entry["cambios_totales"] += int(row.get("cambios_entrada", 0)) + int(row.get("cambios_salida", 0))

    for equipo, entry in aggregate.items():
        partidos = entry.get("partidos") or partidos_por_equipo.get(equipo, 0)
        entry["partidos"] = partidos
        if partidos:
            entry["tarjetas_por_partido"] = round((entry.get("amarillas", 0) + entry.get("rojas", 0)) / partidos, 3)
            entry["cambios_por_partido"] = round(entry.get("cambios_totales", 0) / partidos, 3)
        else:
            entry["tarjetas_por_partido"] = 0.0
            entry["cambios_por_partido"] = 0.0

    return list(aggregate.values())


def save_standings_to_csv(rows: Sequence[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = ["Equipo", "PJ", "W", "D", "L", "GF", "GA", "GD", "Pts"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(
                [
                    row.get("equipo", ""),
                    row.get("pj", 0),
                    row.get("pg", 0),
                    row.get("pe", 0),
                    row.get("pp", 0),
                    row.get("gf", 0),
                    row.get("gc", 0),
                    row.get("dg", row.get("gf", 0) - row.get("gc", 0)),
                    row.get("pts", 0),
                ]
            )


def save_discipline_to_csv(rows: Sequence[dict], path: Path = DISCIPLINE_WORLD_CSV) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = [
        "equipo",
        "partidos",
        "amarillas",
        "rojas",
        "tarjetas_por_partido",
        "cambios_totales",
        "cambios_por_partido",
        "fuente",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in headers})


def load_worldfootball_discipline(csv_path: Path | None = None) -> list[dict]:
    path = csv_path or DISCIPLINE_WORLD_CSV
    if not path.exists():
        raise FileNotFoundError(
            f"No se encontró {path}. Ejecutá primero /standings/refresh o /estadisticas/disciplina/refresh."
        )
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [
            {
                "equipo": row.get("equipo", ""),
                "partidos": int(row.get("partidos", 0) or 0),
                "amarillas": int(row.get("amarillas", 0) or 0),
                "rojas": int(row.get("rojas", 0) or 0),
                "tarjetas_por_partido": float(row.get("tarjetas_por_partido", 0) or 0),
                "cambios_totales": int(row.get("cambios_totales", 0) or 0),
                "cambios_por_partido": float(row.get("cambios_por_partido", 0) or 0),
                "fuente": row.get("fuente", "worldfootball"),
            }
            for row in reader
        ]


__all__ = [
    "fetch_standings_worldfootball",
    "fetch_team_appearances_worldfootball",
    "aggregate_discipline_by_team",
    "save_standings_to_csv",
    "save_discipline_to_csv",
    "DISCIPLINE_WORLD_CSV",
    "load_worldfootball_discipline",
]
