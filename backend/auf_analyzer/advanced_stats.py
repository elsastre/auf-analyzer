from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

from auf_analyzer.services import CSV_STANDINGS, get_team_table


DISCIPLINE_URL = (
    "https://fbref.com/en/comps/45/cards/Primera-Division-Uruguay-Stats"
)
DISCIPLINE_CSV = Path("data") / "team_discipline_uruguay.csv"


def _normalize_team_name(nombre: str, referencias: Iterable[str]) -> str:
    nombre_norm = nombre.strip().lower()
    for ref in referencias:
        if nombre_norm == ref.lower():
            return ref
    for ref in referencias:
        ref_norm = ref.lower()
        if nombre_norm in ref_norm or ref_norm in nombre_norm:
            return ref
    return nombre.strip()


def _select_discipline_table(html: str) -> pd.DataFrame:
    tables = pd.read_html(html)
    for table in tables:
        columns = [str(c) for c in table.columns]
        if any("CrdY" in c or "Cards" in c for c in columns):
            return table
    raise ValueError("No se encontró una tabla de disciplina en la página descargada.")


def _to_int(value) -> int:
    try:
        return int(str(value).replace(",", "").strip())
    except (TypeError, ValueError, AttributeError):
        return 0


def fetch_team_discipline_stats(
    url: str = DISCIPLINE_URL, csv_path: Path | None = None
) -> list[dict]:
    """Descarga estadísticas de disciplina por equipo y las guarda en CSV."""

    if csv_path is None:
        csv_path = DISCIPLINE_CSV

    resp = requests.get(url, timeout=20)
    resp.raise_for_status()

    df_raw = _select_discipline_table(resp.text)
    df_raw.columns = [str(c).split(" ")[-1] if isinstance(c, tuple) else str(c) for c in df_raw.columns]
    df_raw = df_raw.rename(columns={"Squad": "Equipo", "MP": "PJ", "CrdY": "Amarillas", "CrdR": "Rojas"})

    if "Equipo" not in df_raw.columns:
        raise ValueError("La tabla de disciplina no tiene columna de equipos")

    equipos_referencia = [t["name"] for t in get_team_table(csv_path=CSV_STANDINGS)]

    registros: list[dict] = []
    for _, row in df_raw.iterrows():
        equipo_raw = str(row.get("Equipo", "")).strip()
        if not equipo_raw:
            continue

        partidos = _to_int(row.get("PJ", row.get("MP", 0)))
        amarillas = _to_int(row.get("Amarillas", row.get("CrdY", 0)))
        rojas = _to_int(row.get("Rojas", row.get("CrdR", 0)))
        cambios_totales = _to_int(row.get("Subs", row.get("Sub", 0)))

        tarjetas_por_partido = (amarillas + rojas) / partidos if partidos else 0.0
        cambios_por_partido = cambios_totales / partidos if partidos else 0.0

        registros.append(
            {
                "equipo": _normalize_team_name(equipo_raw, equipos_referencia),
                "partidos": partidos,
                "amarillas": amarillas,
                "rojas": rojas,
                "tarjetas_por_partido": round(tarjetas_por_partido, 3),
                "cambios_totales": cambios_totales,
                "cambios_por_partido": round(cambios_por_partido, 3),
                "fuente": "fbref",
            }
        )

    if not registros:
        raise ValueError("No se pudieron parsear filas de disciplina")

    df_out = pd.DataFrame(registros)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(csv_path, index=False)

    return registros


def load_team_discipline_stats(csv_path: Path | None = None) -> list[dict]:
    """Carga el CSV procesado de disciplina por equipo."""

    if csv_path is None:
        csv_path = DISCIPLINE_CSV

    if not csv_path.exists():
        raise FileNotFoundError(
            f"No se encontró {csv_path}. Ejecutá POST /estadisticas/disciplina/refresh."
        )

    df = pd.read_csv(csv_path)
    return df.to_dict(orient="records")
