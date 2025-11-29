from __future__ import annotations

import os
from pathlib import Path
from typing import Optional
import logging

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from auf_analyzer.storage import (
    DEFAULT_SEASON,
    DEFAULT_STAGE,
    compute_table,
    discipline_table,
    ensure_db,
    get_metadata,
    list_fixtures,
    list_match_events,
    list_scorers,
    list_teams_basic,
    player_standard_stats,
    players_overview,
    reseed_database,
    seed_if_needed,
    stats_insights,
    teams_summary,
    teams_list,
)

# ✅ Importación CORRECTA del nuevo ConsultorLibre
from auf_analyzer.consultor_ia_real import consulta_libre_ia, analizar_enfrentamiento

logger = logging.getLogger(__name__)

API_TITLE = "AUF Analyzer API"
DATA_MODE = os.getenv("DATA_MODE", "seed")

app = FastAPI(title=API_TITLE)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ConsultorRequest(BaseModel):
    teamA_id: Optional[int] = None
    teamB_id: Optional[int] = None
    equipo_a: Optional[str] = None
    equipo_b: Optional[str] = None
    season: Optional[int] = None
    stage: Optional[str] = None


class ConsultaLibreRequest(BaseModel):
    consulta: str
    season: Optional[int] = None
    stage: Optional[str] = None


@app.on_event("startup")
def startup() -> None:
    ensure_db()
    seed_if_needed()


def _resolve_params(season: Optional[int], stage: Optional[str]):
    meta = get_metadata()
    resolved_season = season or meta.get("default_season", DEFAULT_SEASON)
    resolved_stage = stage or meta.get("default_stage", DEFAULT_STAGE)
    if resolved_stage not in meta["stages"]:
        raise HTTPException(status_code=400, detail="Etapa no soportada")
    if resolved_season not in meta["seasons"]:
        raise HTTPException(status_code=400, detail="Temporada no soportada")
    return resolved_season, resolved_stage, meta


@app.get("/meta")
def api_meta():
    return get_metadata()


@app.get("/tables")
def api_tables(season: Optional[int] = None, stage: Optional[str] = None):
    resolved_season, resolved_stage, _ = _resolve_params(season, stage)
    return compute_table(resolved_season, resolved_stage)


@app.get("/teams")
def api_teams():
    return {"teams": list_teams_basic()}


@app.get("/teams/summary")
def api_teams_summary(season: Optional[int] = None, stage: Optional[str] = None):
    resolved_season, resolved_stage, _ = _resolve_params(season, stage)
    return {
        "season": resolved_season,
        "stage": resolved_stage,
        "teams": teams_summary(resolved_season, resolved_stage),
    }


@app.get("/fixtures")
def api_fixtures(
    season: Optional[int] = None,
    stage: Optional[str] = None,
    team_id: Optional[int] = Query(None, description="Filtro opcional de equipo"),
    round: Optional[str] = Query(None, description="Ronda a filtrar"),
):
    resolved_season, resolved_stage, _ = _resolve_params(season, stage)
    return {
        "season": resolved_season,
        "stage": resolved_stage,
        "fixtures": list_fixtures(
            resolved_season, resolved_stage, team_id=team_id, round_number=round
        ),
        "source": "seed",
    }


@app.get("/scorers")
def api_scorers(
    season: Optional[int] = None,
    stage: Optional[str] = None,
    top: int = Query(20, ge=1, le=100),
):
    resolved_season, resolved_stage, _ = _resolve_params(season, stage)
    return {
        "season": resolved_season,
        "stage": resolved_stage,
        "scorers": list_scorers(resolved_season, resolved_stage, top=top),
        "source": "seed",
    }


@app.get("/matches/{match_id}/events")
def api_match_events(match_id: int):
    return {"match_id": match_id, "events": list_match_events(match_id)}


@app.get("/stats/insights")
def api_stats(season: Optional[int] = None, stage: Optional[str] = None):
    resolved_season, resolved_stage, _ = _resolve_params(season, stage)
    return stats_insights(resolved_season, resolved_stage)


@app.get("/players")
def api_players(
    season: Optional[int] = None,
    stage: Optional[str] = None,
    team_id: Optional[int] = Query(None, description="Filtrar por equipo"),
):
    resolved_season, resolved_stage, _ = _resolve_params(season, stage)
    rows = players_overview(resolved_season, resolved_stage, team_id=team_id)
    return {"season": resolved_season, "stage": resolved_stage, "players": rows}


@app.post("/ai/consultor")
def api_ai_consultor(payload: ConsultorRequest):
    """Endpoint para comparación de equipos - USA NUEVO CONSULTOR"""
    resolved_season, resolved_stage, _ = _resolve_params(
        payload.season, payload.stage
    )
    meta = get_metadata()
    name_map = {t["id"]: t["name"] for t in meta.get("teams", [])}
    team_a = payload.equipo_a or name_map.get(payload.teamA_id)
    team_b = payload.equipo_b or name_map.get(payload.teamB_id)
    
    if not team_a or not team_b:
        raise HTTPException(status_code=400, detail="Se requieren dos equipos válidos")
    
    try:
        # ✅ USA el NUEVO ConsultorLibre
        return analizar_enfrentamiento(team_a, team_b, resolved_season, resolved_stage)
    except Exception as exc:
        logger.error(f"Error en consultor: {exc}")
        raise HTTPException(status_code=500, detail=f"Error en consultor: {str(exc)}")


@app.get("/standings")
def api_standings(season: Optional[int] = None, stage: Optional[str] = None):
    resolved_season, resolved_stage, _ = _resolve_params(season, stage)
    return compute_table(resolved_season, resolved_stage)


@app.get("/standings/refresh")
async def api_refresh_standings():
    if DATA_MODE != "scrape":
        return {
            "warning": "Modo demo activo: se usan datos seed locales",
            "mode": DATA_MODE,
            "data": compute_table(DEFAULT_SEASON, DEFAULT_STAGE),
        }
    return {
        "message": "Modo scrape no implementado en demo",
        "mode": DATA_MODE,
        "data": compute_table(DEFAULT_SEASON, DEFAULT_STAGE),
    }


@app.get("/players/stats")
def api_players_stats(
    season: Optional[int] = None,
    stage: Optional[str] = None,
    team_id: Optional[int] = Query(None, description="Filtrar por equipo"),
):
    resolved_season, resolved_stage, _ = _resolve_params(season, stage)
    rows = player_standard_stats(resolved_season, resolved_stage, team_id=team_id)
    return {"season": resolved_season, "stage": resolved_stage, "players": rows}


@app.post("/admin/reseed")
def api_reseed(hard: bool = Query(False)):
    if os.getenv("ALLOW_RESEED", "false").lower() != "true":
        raise HTTPException(status_code=403, detail="Reseed no habilitado")
    reseed_database(hard=hard)
    return {"message": "Reseed ejecutado", "hard": hard}


@app.get("/goleadores")
def api_goleadores(top: int = Query(20, ge=1, le=100)):
    scorers = api_scorers(top=top)["scorers"]
    return {
        "count": len(scorers),
        "goleadores": [
            {"Jugador": row["player"], "Equipo": row["team"], "Goles": row["goals"]}
            for row in scorers
        ],
    }


@app.get("/estadisticas/disciplina")
def api_disciplina(season: Optional[int] = None, stage: Optional[str] = None):
    resolved_season, resolved_stage, _ = _resolve_params(season, stage)
    rows = discipline_table(resolved_season, resolved_stage)
    return {"count": len(rows), "equipos": rows, "source": "seed"}


@app.get("/estadisticas/disciplina/refresh")
async def api_refresh_disciplina():
    if DATA_MODE != "scrape":
        return {
            "warning": "Modo demo activo: se usan datos seed locales",
            "mode": DATA_MODE,
            "equipos": discipline_table(DEFAULT_SEASON, DEFAULT_STAGE),
        }
    return {
        "message": "Modo scrape no implementado en demo",
        "mode": DATA_MODE,
        "equipos": discipline_table(DEFAULT_SEASON, DEFAULT_STAGE),
    }


@app.get("/torneo/equipos")
def api_list_equipos():
    table = compute_table(DEFAULT_SEASON, DEFAULT_STAGE)
    equipos = [
        {
            "name": row["team"],
            "mp": row["pj"],
            "w": row["pg"],
            "d": row["pe"],
            "l": row["pp"],
            "gf": row["gf"],
            "ga": row["gc"],
            "gd": row["dg"],
            "pts": row["pts"],
        }
        for row in table["rows"]
    ]
    return {"count": len(equipos), "equipos": equipos}


@app.get("/torneo/equipos/buscar")
def api_buscar_equipo(nombre: str):
    equipos = teams_list()
    result = [e for e in equipos if nombre.lower() in e.lower()]
    if not result:
        raise HTTPException(status_code=404, detail="Equipo no encontrado")
    return {"resultados": result}


@app.get("/torneo/ranking")
def api_ranking_equipos():
    table = compute_table(DEFAULT_SEASON, DEFAULT_STAGE)
    return {"count": len(table["rows"]), "equipos": table["rows"]}


@app.get("/torneo/mejores-ataques")
def api_mejores_ataques(top: int = 5):
    table = compute_table(DEFAULT_SEASON, DEFAULT_STAGE)
    sorted_rows = sorted(table["rows"], key=lambda r: (-r["gf"], r["team"]))
    return {"count": len(sorted_rows[:top]), "equipos": sorted_rows[:top]}


@app.post("/ai/consulta-libre")
def api_consulta_libre(payload: ConsultaLibreRequest):
    """🤖 NUEVO ENDPOINT - Consultas con IA REAL"""
    try:
        resolved_season, resolved_stage, _ = _resolve_params(
            payload.season, payload.stage
        )
        
        # Agregar contexto de temporada/torneo a la consulta si no está especificado
        consulta_con_contexto = payload.consulta
        consulta_str = str(payload.consulta)
        if str(resolved_season) not in consulta_str and resolved_stage not in consulta_str:
            consulta_con_contexto = f"{payload.consulta} (temporada {resolved_season}, {resolved_stage})"
        
        # 🔥 USA EL NUEVO MÓDULO CON IA REAL
        resultado = consulta_libre_ia(consulta_con_contexto)
        return resultado
    except Exception as exc:
        import traceback
        logger.error(f"Error en consulta libre: {exc}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error en consulta libre: {str(exc)}")
    
@app.post("/ai/consultor")
def api_ai_consultor(payload: ConsultorRequest):
    """Endpoint para comparación de equipos - USA IA REAL"""
    resolved_season, resolved_stage, _ = _resolve_params(
        payload.season, payload.stage
    )
    meta = get_metadata()
    name_map = {t["id"]: t["name"] for t in meta.get("teams", [])}
    
    team_a = payload.equipo_a or name_map.get(payload.teamA_id)
    team_b = payload.equipo_b or name_map.get(payload.teamB_id)
    
    if not team_a or not team_b:
        raise HTTPException(status_code=400, detail="Se requieren dos equipos válidos")
    
    try:
        # 🔥 USA EL NUEVO MÓDULO CON IA REAL
        return analizar_enfrentamiento(team_a, team_b, resolved_season, resolved_stage)
    except Exception as exc:
        logger.error(f"Error en consultor: {exc}")
        logger.error(traceback.format_exc())