from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Optional
from dataclasses import dataclass
import itertools as it
import re
import pandas as pd

# Ruta centralizada del CSV generado por el scraper
CSV_STANDINGS = Path("data") / "standings_uruguay.csv"
SAMPLE_STANDINGS_CSV = Path("data") / "standings_uruguay_sample.csv"

# Metadatos de clubes (apodo + estadio).
# Si en el CSV aparece con ese nombre, se enriquecen con estos datos.
TEAM_META: dict[str, dict[str, str]] = {
    "Nacional": {
        "nickname": "Bolso",
        "stadium": "Gran Parque Central",
    },
    "Peñarol": {
        "nickname": "Carbonero",
        "stadium": "Campeón del Siglo",
    },
    "Liverpool": {
        "nickname": "Negriazul",
        "stadium": "Belvedere",
    },
    "Juventud de Las Piedras": {
        "nickname": "Juve",
        "stadium": "Parque Artigas Las Piedras",
    },
    "Defensor": {
        "nickname": "Violeta",
        "stadium": "Luis Franzini",
    },
    "Boston River": {
        "nickname": "Boston",
        "stadium": "Parque Artigas",
    },
    "Racing": {
        "nickname": "Cervecero",
        "stadium": "Osvaldo Roberto",
    },
    "Torque": {
        "nickname": "Ciudadano",
        "stadium": "Parque Viera (localía compartida)",
    },
    "Cerro Largo": {
        "nickname": "Arachán",
        "stadium": "Arquitecto Antonio Ubilla",
    },
    "Cerro": {
        "nickname": "Villero",
        "stadium": "Luis Tróccoli",
    },
    "Danubio": {
        "nickname": "La Franja",
        "stadium": "Jardines del Hipódromo",
    },
    "Progreso": {
        "nickname": "Gaucho del Pantanoso",
        "stadium": "Abraham Paladino",
    },
    "Plaza Colonia": {
        "nickname": "Patablanca",
        "stadium": "Profesor Alberto Suppici",
    },
    "Wanderers": {
        "nickname": "Bohemio",
        "stadium": "Parque Viera",
    },
    "Miramar Misiones": {
        "nickname": "Cebrita",
        "stadium": "Méndez Piana",
    },
    "River Plate": {
        "nickname": "Darsenero",
        "stadium": "Parque Saroldi",
    },
}


def _resolve_standings_csv(preferred: Path | None = None) -> Path | None:
    """Determina qué CSV usar para cargar standings.

    Prioridad:
    1) Un CSV explícito (``preferred``) si existe y tiene contenido.
    2) ``CSV_STANDINGS`` si existe y tiene contenido.
    3) ``SAMPLE_STANDINGS_CSV`` si existe (aunque sea pequeño).
    4) ``None`` si no hay datos disponibles.
    """

    def _usable_csv(candidate: Path | None) -> Path | None:
        if candidate is None:
            return None
        if not candidate.exists():
            return None
        try:
            if candidate.stat().st_size <= 0:
                return None
        except OSError:
            return None
        return candidate

    for option in (preferred, CSV_STANDINGS, SAMPLE_STANDINGS_CSV):
        usable = _usable_csv(option)
        if usable is not None:
            return usable

    return None


def _load_rows(csv_path: Path | None = None) -> tuple[list[str], list[list[str]]]:
    """
    Lee el CSV y devuelve (headers, filas).
    """
    resolved = _resolve_standings_csv(csv_path)
    if resolved is None:
        # Modo sin datos: devolvemos estructuras vacías pero sin romper.
        return [], []

    csv_path = resolved

    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        raise ValueError("El CSV de standings está vacío.")

    headers = rows[0]
    data_rows = rows[1:]
    return headers, data_rows


def _build_index(headers: list[str]) -> dict[str, int]:
    """
    Construye un índice de columnas buscando nombres típicos
    (FBref, español, etc.).
    """
    norm_headers = [h.strip().lower() for h in headers]

    def find_column(posibles: list[str]) -> Optional[int]:
        for name in posibles:
            name_norm = name.strip().lower()
            if name_norm in norm_headers:
                return norm_headers.index(name_norm)
        return None

    mapping: dict[str, list[str]] = {
        "team": ["squad", "equipo", "team"],
        "mp": ["mp", "pj", "matches"],
        "w": ["w", "g", "wins", "pg"],
        "d": ["d", "e", "draws", "pe"],
        "l": ["l", "p", "losses", "pp"],
        "gf": ["gf", "f", "goles a favor"],
        "ga": ["ga", "a", "goles en contra", "gc"],
        "pts": ["pts", "puntos"],
    }

    index: dict[str, int] = {}
    for key, posibles in mapping.items():
        col_idx = find_column(posibles)
        if col_idx is None:
            raise KeyError(
                f"No se encontró ninguna columna {posibles} en los encabezados del CSV: {headers}"
            )
        index[key] = col_idx

    return index


def _to_int(value: str) -> int:
    try:
        return int(value.replace(",", ""))
    except (TypeError, ValueError, AttributeError):
        return 0


def load_standings_df(csv_path: Path | None = None) -> pd.DataFrame:
    """
    Carga el CSV de standings como DataFrame de pandas.
    """
    resolved = _resolve_standings_csv(csv_path)
    if resolved is None:
        raise FileNotFoundError(
            "No hay CSV de standings disponible (ni generado ni de ejemplo)."
        )

    return pd.read_csv(resolved, encoding="utf-8")


def get_team_table(csv_path: Path | None = None) -> list[dict[str, Any]]:
    """
    Devuelve una lista de dicts con stats básicas por equipo.

    Cada elemento tiene la forma:
    {
        "name": str,
        "mp": int,
        "w": int,
        "d": int,
        "l": int,
        "gf": int,
        "ga": int,
        "pts": int,
        "gd": int,
        "nickname": str | None,
        "stadium": str | None,
    }
    """
    headers, rows = _load_rows(csv_path)
    if not headers:
        return []

    idx = _build_index(headers)

    equipos: list[dict[str, Any]] = []

    for row in rows:
        if not row:
            continue

        nombre = row[idx["team"]].strip()
        if not nombre:
            continue

        mp = _to_int(row[idx["mp"]])
        w = _to_int(row[idx["w"]])
        d = _to_int(row[idx["d"]])
        l = _to_int(row[idx["l"]])
        gf = _to_int(row[idx["gf"]])
        ga = _to_int(row[idx["ga"]])
        pts = _to_int(row[idx["pts"]])

        meta = TEAM_META.get(nombre, {})

        equipos.append(
            {
                "name": nombre,
                "mp": mp,
                "w": w,
                "d": d,
                "l": l,
                "gf": gf,
                "ga": ga,
                "pts": pts,
                "gd": gf - ga,
                "nickname": meta.get("nickname"),
                "stadium": meta.get("stadium"),
            }
        )

    return equipos


def buscar_equipo_por_nombre(nombre: str, csv_path: Path | None = None) -> Optional[dict[str, Any]]:
    """
    Devuelve el primer equipo cuyo nombre contenga el texto buscado (case-insensitive).
    """
    nombre_norm = nombre.strip().lower()
    if not nombre_norm:
        return None

    equipos = get_team_table(csv_path)
    for equipo in equipos:
        if nombre_norm in equipo["name"].lower():
            return equipo

    return None


def ranking_equipos_por_puntos(csv_path: Path | None = None) -> list[dict[str, Any]]:
    """
    Devuelve todos los equipos ordenados por puntos (y diferencia de goles como desempate).
    """
    equipos = get_team_table(csv_path)
    return sorted(
        equipos,
        key=lambda e: (e["pts"], e["gd"], e["gf"]),
        reverse=True,
    )


def mejores_ataques(top: int = 5, csv_path: Path | None = None) -> list[dict[str, Any]]:
    """
    Devuelve los equipos con más goles a favor.
    """
    equipos = get_team_table(csv_path)
    equipos_ordenados = sorted(equipos, key=lambda e: e["gf"], reverse=True)
    return equipos_ordenados[:max(top, 1)]


# --- Goleadores: procesamiento funcional a partir del CSV de standings ---

# CSV derivado que vamos a generar
TOP_SCORERS_CSV = (
    Path(__file__).resolve().parents[1] / "data" / "top_scorers_uruguay.csv"
)


@dataclass(frozen=True)
class Goleador:
    jugador: str
    equipo: str
    goles: int


def _normalize_col_name(name: str) -> str:
    """Normaliza nombres de columnas (sin espacios, minúsculas)."""
    return re.sub(r"[\s_]+", "", str(name)).strip().lower()


def _find_column(df: pd.DataFrame, logical_name_options: list[str]) -> str:
    """
    Busca en el DataFrame una columna que matchee alguna de las opciones lógicas
    usando normalización. Ej: ["squad", "equipo"].
    """
    normalized_targets = { _normalize_col_name(opt) for opt in logical_name_options }

    for col in df.columns:
        if _normalize_col_name(col) in normalized_targets:
            return col

    raise KeyError(
        f"No se encontró ninguna columna equivalente a {logical_name_options} en el CSV."
    )


def _parse_top_scorers_cell(cell: str, team: str) -> list[Goleador]:
    """
    Recibe algo tipo:
      "Nicolás López-20"
      "Agustín Albarracín,Augustín Anello-9"
    y devuelve una lista de Goleador (uno por jugador).
    """
    if not isinstance(cell, str) or not cell.strip():
        return []

    # Tomamos el último '-' como separador entre nombres y goles
    try:
        names_part, goals_part = cell.rsplit("-", 1)
    except ValueError:
        # Formato inesperado, lo ignoramos para no romper todo
        return []

    goals_match = re.search(r"\d+", goals_part)
    if not goals_match:
        return []

    goles = int(goals_match.group())
    # Varios nombres separados por coma
    nombres = [n.strip() for n in names_part.split(",") if n.strip()]

    return [Goleador(jugador=nombre, equipo=team, goles=goles) for nombre in nombres]


def top_scorers(limit: int | None = None, csv_path: Path | None = None) -> list[dict]:
    """
    Calcula goleadores a partir del CSV de standings, guarda un CSV derivado
    y devuelve una lista de dicts ordenados por goles (desc).
    """
    # Reutilizamos la función existente que ya lee standings_uruguay.csv
    df = load_standings_df(csv_path=csv_path)

    # Detectamos columnas de equipo y goleadores de forma tolerante
    col_equipo = _find_column(df, ["squad", "equipo"])
    col_scorer = _find_column(df, ["topteamscorer", "top team scorer", "goleador"])

    # Generamos lista de Goleador usando enfoque funcional
    goleadores_iterables = (
        _parse_top_scorers_cell(row[col_scorer], str(row[col_equipo]))
        for _, row in df.iterrows()
    )
    goleadores_flat: list[Goleador] = list(it.chain.from_iterable(goleadores_iterables))

    # Por si un jugador aparece repetido (no debería, pero por las dudas agregamos)
    acumulado: dict[tuple[str, str], int] = {}
    for g in goleadores_flat:
        key = (g.jugador, g.equipo)
        acumulado[key] = acumulado.get(key, 0) + g.goles

    goleadores_final = [
        Goleador(jugador=j, equipo=e, goles=g)
        for (j, e), g in acumulado.items()
    ]

    # Ordenamos de mayor a menor
    goleadores_ordenados = sorted(
        goleadores_final,
        key=lambda g: g.goles,
        reverse=True,
    )

    if limit is not None and limit > 0:
        goleadores_ordenados = goleadores_ordenados[:limit]

    # Lo convertimos a DataFrame para guardar CSV derivado
    df_out = pd.DataFrame(
        [
            {"Jugador": g.jugador, "Equipo": g.equipo, "Goles": g.goles}
            for g in goleadores_ordenados
        ]
    )
    # Guardamos el CSV derivado
    TOP_SCORERS_CSV.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(TOP_SCORERS_CSV, index=False)

    # Y devolvemos como lista de dicts para la API
    return df_out.to_dict(orient="records")