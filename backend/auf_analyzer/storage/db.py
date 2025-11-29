from __future__ import annotations

import json
import random
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Sequence

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
DB_PATH = DATA_DIR / "auf.db"
SEEDS_DIR = DATA_DIR / "seeds"
SCHEMA_VERSION = 3

DEFAULT_SEASON = 2024
DEFAULT_STAGE = "apertura"
STAGE_NAMES = {
    "apertura": "Torneo Apertura",
    "clausura": "Torneo Clausura",
    "intermedio": "Torneo Intermedio",
    "anual": "Tabla Anual",
}
REFEREES = [
    "Esteban Ostojich",
    "Andrés Matonte",
    "Gustavo Tejera",
    "Leodán González",
    "Christian Ferreyra",
    "Daniel Fedorczuk",
]
KICKOFF_TIMES = ["15:00", "16:00", "18:00", "20:30"]


@dataclass
class Team:
    id: int
    name: str
    short_name: str
    city: str
    stadium: str
    logo_key: str
    is_placeholder: bool = False


@dataclass
class MatchResult:
    match_id: int
    season_year: int
    stage_code: str
    home_team_id: int
    away_team_id: int
    home_goals: int
    away_goals: int
    date: str


def get_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    path = db_path or DB_PATH
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_db(db_path: Optional[Path] = None) -> None:
    path = db_path or DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        conn = get_connection(path)
        create_schema(conn)
        seed_database(conn)
        conn.close()


def seed_if_needed(db_path: Optional[Path] = None) -> None:
    path = db_path or DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    initialize = not path.exists()
    if initialize:
        ensure_db(path)
        return

    conn = get_connection(path)
    try:
        meta_ok = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='metadata'"
        ).fetchone()
        version = 0
        if meta_ok:
            row = conn.execute(
                "SELECT value FROM metadata WHERE key='schema_version'"
            ).fetchone()
            version = int(row[0]) if row else 0
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='matches'"
        ).fetchall()
        if version < SCHEMA_VERSION or not tables:
            conn.close()
            path.unlink(missing_ok=True)
            ensure_db(path)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;
        DROP TABLE IF EXISTS match_events;
        DROP TABLE IF EXISTS player_match_stats;
        DROP TABLE IF EXISTS matches;
        DROP TABLE IF EXISTS players;
        DROP TABLE IF EXISTS standings;
        DROP TABLE IF EXISTS teams;
        DROP TABLE IF EXISTS seasons;
        DROP TABLE IF EXISTS stages;
        DROP TABLE IF EXISTS metadata;

        CREATE TABLE metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE teams (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            short_name TEXT,
            city TEXT,
            stadium TEXT,
            logo_key TEXT,
            is_placeholder INTEGER DEFAULT 0
        );

        CREATE TABLE seasons (
            year INTEGER PRIMARY KEY
        );

        CREATE TABLE stages (
            code TEXT PRIMARY KEY,
            name TEXT NOT NULL
        );

        CREATE TABLE matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            season_year INTEGER NOT NULL,
            stage_code TEXT NOT NULL,
            round TEXT NOT NULL,
            date TEXT NOT NULL,
            time TEXT NOT NULL,
            home_team_id INTEGER NOT NULL,
            away_team_id INTEGER NOT NULL,
            home_goals INTEGER NOT NULL,
            away_goals INTEGER NOT NULL,
            home_xg REAL,
            away_xg REAL,
            attendance INTEGER,
            venue TEXT,
            referee TEXT,
            FOREIGN KEY(home_team_id) REFERENCES teams(id),
            FOREIGN KEY(away_team_id) REFERENCES teams(id),
            FOREIGN KEY(season_year) REFERENCES seasons(year),
            FOREIGN KEY(stage_code) REFERENCES stages(code)
        );

        CREATE TABLE players (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL,
            team_id INTEGER NOT NULL,
            position TEXT,
            nationality TEXT,
            birth_year INTEGER,
            FOREIGN KEY(team_id) REFERENCES teams(id)
        );

        CREATE TABLE match_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id INTEGER NOT NULL,
            minute INTEGER NOT NULL,
            team_id INTEGER NOT NULL,
            player_id INTEGER,
            type TEXT NOT NULL,
            detail TEXT,
            FOREIGN KEY(match_id) REFERENCES matches(id),
            FOREIGN KEY(team_id) REFERENCES teams(id),
            FOREIGN KEY(player_id) REFERENCES players(id)
        );

        CREATE TABLE player_match_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id INTEGER NOT NULL,
            player_id INTEGER NOT NULL,
            team_id INTEGER NOT NULL,
            minutes INTEGER NOT NULL,
            goals INTEGER DEFAULT 0,
            assists INTEGER DEFAULT 0,
            shots INTEGER DEFAULT 0,
            shots_on_target INTEGER DEFAULT 0,
            xg REAL DEFAULT 0,
            xa REAL DEFAULT 0,
            yellow INTEGER DEFAULT 0,
            red INTEGER DEFAULT 0,
            starts INTEGER DEFAULT 0,
            FOREIGN KEY(match_id) REFERENCES matches(id),
            FOREIGN KEY(player_id) REFERENCES players(id),
            FOREIGN KEY(team_id) REFERENCES teams(id)
        );

        CREATE TABLE standings (
            season_year INTEGER NOT NULL,
            stage_code TEXT NOT NULL,
            team_id INTEGER NOT NULL,
            mp INTEGER NOT NULL,
            w INTEGER NOT NULL,
            d INTEGER NOT NULL,
            l INTEGER NOT NULL,
            gf INTEGER NOT NULL,
            ga INTEGER NOT NULL,
            gd INTEGER NOT NULL,
            pts INTEGER NOT NULL,
            ppg REAL NOT NULL,
            last5 TEXT,
            top_scorer TEXT,
            goalkeeper TEXT,
            avg_attendance REAL,
            PRIMARY KEY (season_year, stage_code, team_id),
            FOREIGN KEY(team_id) REFERENCES teams(id)
        );

        CREATE INDEX idx_matches_stage ON matches(season_year, stage_code);
        CREATE INDEX idx_events_match ON match_events(match_id);
        CREATE INDEX idx_player_stats_match ON player_match_stats(match_id);
        """
    )
    conn.commit()


def seed_database(conn: sqlite3.Connection) -> None:
    teams = _load_teams()
    seasons = _load_roster_seasons()
    _insert_base_rows(conn, teams, seasons)

    for season in seasons:
        rng = random.Random(42 + season)
        roster = _load_rosters_for_season(season)
        player_map = _insert_players(conn, teams, roster)
        fixtures = _generate_fixture_blocks(teams, season, rng)
        all_matches: List[MatchResult] = []
        events_rows: list[tuple] = []
        stats_rows: list[tuple] = []
        for stage_code, schedule in fixtures.items():
            for match_info in schedule:
                match_row, match_events, match_stats = _simulate_match(
                    conn,
                    rng,
                    season,
                    stage_code,
                    match_info,
                    player_map,
                    teams,
                )
                all_matches.append(
                    MatchResult(
                        match_id=match_row[0],
                        season_year=season,
                        stage_code=stage_code,
                        home_team_id=match_info["home"],
                        away_team_id=match_info["away"],
                        home_goals=match_info["home_goals"],
                        away_goals=match_info["away_goals"],
                        date=match_info["date"].isoformat(),
                    )
                )
                events_rows.extend(match_events)
                stats_rows.extend(match_stats)
        _persist_events(conn, events_rows)
        _persist_player_stats(conn, stats_rows)
        _build_and_store_standings(conn, season, all_matches)

    conn.execute(
        "INSERT OR REPLACE INTO metadata(key, value) VALUES('schema_version', ?)",
        (SCHEMA_VERSION,),
    )
    conn.commit()


def _load_teams() -> list[Team]:
    seed_file = SEEDS_DIR / "teams.json"
    data = json.loads(seed_file.read_text())
    teams = []
    for entry in data.get("teams", []):
        teams.append(
            Team(
                id=int(entry["id"]),
                name=entry["name"],
                short_name=entry.get("short_name", entry["name"][:3]),
                city=entry.get("city", ""),
                stadium=entry.get("stadium", ""),
                logo_key=entry.get("logo_key", ""),
                is_placeholder=bool(entry.get("is_placeholder", False)),
            )
        )
    return teams


def _load_roster_seasons() -> list[int]:
    seasons = []
    for path in SEEDS_DIR.glob("rosters_*.json"):
        try:
            season = int(path.stem.split("_")[1])
            seasons.append(season)
        except ValueError:
            continue
    return sorted(seasons)


def _load_rosters_for_season(season: int) -> dict[str, dict]:
    path = SEEDS_DIR / f"rosters_{season}.json"
    data = json.loads(path.read_text())
    teams_data = {}
    for team_entry in data.get("teams", []):
        teams_data[team_entry["name"]] = team_entry
    return teams_data


def _insert_base_rows(conn: sqlite3.Connection, teams: list[Team], seasons: list[int]):
    conn.executemany(
        "INSERT INTO teams(id, name, short_name, city, stadium, logo_key, is_placeholder) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (
                t.id,
                t.name,
                t.short_name,
                t.city,
                t.stadium,
                t.logo_key,
                int(t.is_placeholder),
            )
            for t in teams
        ],
    )
    conn.executemany("INSERT INTO seasons(year) VALUES (?)", [(s,) for s in seasons])
    conn.executemany(
        "INSERT INTO stages(code, name) VALUES(?, ?)",
        [(code, name) for code, name in STAGE_NAMES.items()],
    )
    conn.commit()


def _insert_players(
    conn: sqlite3.Connection,
    teams: list[Team],
    roster: dict[str, dict],
) -> dict[int, list[int]]:
    def _normalize_nationality(code: Optional[str]) -> Optional[str]:
        if not code:
            return None
        normalized = code.strip().upper()
        map3_to_2 = {
            "URU": "UY",
            "ARG": "AR",
            "BRA": "BR",
            "COL": "CO",
            "PAR": "PY",
            "CHI": "CL",
        }
        if len(normalized) == 3 and normalized in map3_to_2:
            return map3_to_2[normalized]
        if len(normalized) == 2:
            return normalized
        return normalized[:2]

    team_lookup = {t.name: t.id for t in teams}
    player_map: dict[int, list[int]] = {t.id: [] for t in teams}
    for team_name, team_data in roster.items():
        team_id = team_lookup.get(team_name)
        if not team_id:
            continue
        rows = []
        for player in team_data.get("players", []):
            name = player["full_name"]
            if "placeholder" in name.lower():
                raise ValueError("Seed inválida: nombre Placeholder detectado")
            rows.append(
                (
                    name,
                    team_id,
                    player.get("position"),
                    _normalize_nationality(player.get("nationality")),
                    player.get("birth_year"),
                )
            )
        conn.executemany(
            "INSERT INTO players(full_name, team_id, position, nationality, birth_year) VALUES (?, ?, ?, ?, ?)",
            rows,
        )
        ids = [r[0] for r in conn.execute("SELECT id FROM players WHERE team_id=?", (team_id,)).fetchall()]
        player_map[team_id] = ids
    conn.commit()
    return player_map


def _generate_fixture_blocks(
    teams: list[Team], season: int, rng: random.Random
) -> dict[str, list[dict]]:
    team_ids = [t.id for t in teams]
    schedule = {
        "apertura": _build_round_robin_schedule(team_ids, season, rng, datetime(season, 2, 10)),
        "clausura": _build_round_robin_schedule(team_ids, season, rng, datetime(season, 8, 10)),
    }
    schedule["intermedio"] = _build_intermedio_schedule(team_ids, season, rng)
    return schedule


def _build_round_robin_schedule(
    team_ids: list[int], season: int, rng: random.Random, start_date: datetime
) -> list[dict]:
    rounds = _round_robin(team_ids)
    fixtures: list[dict] = []
    for idx, pairings in enumerate(rounds, start=1):
        round_date = start_date + timedelta(days=7 * (idx - 1))
        for home, away in pairings:
            home_advantage = rng.uniform(0, 1)
            base_goals = rng.randint(0, 3)
            home_goals = base_goals + (1 if home_advantage > 0.6 else 0)
            away_goals = rng.randint(0, 3)
            fixtures.append(
                {
                    "round": str(idx),
                    "date": round_date.date(),
                    "time": rng.choice(KICKOFF_TIMES),
                    "home": home,
                    "away": away,
                    "home_goals": home_goals,
                    "away_goals": away_goals,
                }
            )
    return fixtures


def _build_intermedio_schedule(
    team_ids: list[int], season: int, rng: random.Random
) -> list[dict]:
    sorted_ids = sorted(team_ids)
    group_a = sorted_ids[:8]
    group_b = sorted_ids[8:]
    start_date = datetime(season, 6, 5)
    fixtures: list[dict] = []
    for group in [group_a, group_b]:
        rounds = _round_robin(group)
        for idx, pairings in enumerate(rounds, start=1):
            round_date = start_date + timedelta(days=7 * (idx - 1))
            for home, away in pairings:
                fixtures.append(
                    {
                        "round": str(idx),
                        "date": round_date.date(),
                        "time": rng.choice(KICKOFF_TIMES),
                        "home": home,
                        "away": away,
                        "home_goals": rng.randint(0, 3),
                        "away_goals": rng.randint(0, 3),
                    }
                )
    # final
    fixtures.append(
        {
            "round": "Final",
            "date": start_date + timedelta(days=7 * 8),
            "time": rng.choice(KICKOFF_TIMES),
            "home": group_a[0],
            "away": group_b[0],
            "home_goals": rng.randint(0, 3),
            "away_goals": rng.randint(0, 3),
        }
    )
    return fixtures


def _round_robin(team_ids: list[int]) -> list[list[tuple[int, int]]]:
    ids = team_ids[:]
    if len(ids) % 2:
        ids.append(-1)
    n = len(ids)
    rounds: list[list[tuple[int, int]]] = []
    for i in range(n - 1):
        pairings = []
        for j in range(n // 2):
            home = ids[j]
            away = ids[n - 1 - j]
            if home == -1 or away == -1:
                continue
            if i % 2 == 0:
                pairings.append((home, away))
            else:
                pairings.append((away, home))
        rounds.append(pairings)
        ids = [ids[0]] + [ids[-1]] + ids[1:-1]
    return rounds


def _attendance_for_team(team_id: int) -> tuple[int, int]:
    big = {1, 2}
    solid = {3, 4, 5, 6, 10}
    if team_id in big:
        return (18000, 35000)
    if team_id in solid:
        return (8000, 18000)
    return (500, 12000)


def _simulate_match(
    conn: sqlite3.Connection,
    rng: random.Random,
    season: int,
    stage_code: str,
    match_info: dict,
    player_map: dict[int, list[int]],
    teams: list[Team],
) -> tuple[tuple, list[tuple], list[tuple]]:
    venue = next((t.stadium for t in teams if t.id == match_info["home"]), "")
    min_att, max_att = _attendance_for_team(match_info["home"])
    attendance = rng.randint(min_att, max_att)
    home_xg = round(match_info["home_goals"] * 0.7 + rng.uniform(0, 1.5), 2)
    away_xg = round(match_info["away_goals"] * 0.7 + rng.uniform(0, 1.4), 2)
    match_row = conn.execute(
        """
        INSERT INTO matches(
            season_year, stage_code, round, date, time, home_team_id, away_team_id,
            home_goals, away_goals, home_xg, away_xg, attendance, venue, referee
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            season,
            stage_code,
            match_info["round"],
            match_info["date"].isoformat(),
            match_info["time"],
            match_info["home"],
            match_info["away"],
            match_info["home_goals"],
            match_info["away_goals"],
            home_xg,
            away_xg,
            attendance,
            venue,
            rng.choice(REFEREES),
        ),
    )
    match_id = match_row.lastrowid

    events = []
    stats_rows: list[tuple] = []
    for team_id, goals, xg in [
        (match_info["home"], match_info["home_goals"], home_xg),
        (match_info["away"], match_info["away_goals"], away_xg),
    ]:
        lineup, bench = _lineup_for_team(team_id, player_map, rng)
        goals_by_player = _allocate_goals(lineup, goals, rng)
        assists_by_player = _allocate_assists(lineup, goals_by_player, rng)
        yellows = _random_cards(lineup, rng, "yellow")
        reds = _random_cards(lineup, rng, "red")
        events.extend(
            _events_for_team(
                match_id, team_id, goals_by_player, yellows, reds, bench, rng
            )
        )
        stats_rows.extend(
            _player_stat_rows(
                match_id,
                team_id,
                lineup,
                bench,
                goals_by_player,
                assists_by_player,
                yellows,
                reds,
                xg,
                rng,
            )
        )
    return (match_id,), events, stats_rows


def _lineup_for_team(
    team_id: int, player_map: dict[int, list[int]], rng: random.Random
) -> tuple[list[int], list[int]]:
    players = player_map.get(team_id, [])
    if len(players) <= 11:
        return players, []
    starters = rng.sample(players, 11)
    bench_candidates = [p for p in players if p not in starters]
    subs = rng.sample(bench_candidates, min(3, len(bench_candidates)))
    return starters, subs


def _allocate_goals(players: list[int], goals: int, rng: random.Random) -> dict[int, int]:
    result: dict[int, int] = {}
    if not players or goals == 0:
        return result
    for _ in range(goals):
        scorer = rng.choice(players)
        result[scorer] = result.get(scorer, 0) + 1
    return result


def _allocate_assists(
    players: list[int], goals_by_player: dict[int, int], rng: random.Random
) -> dict[int, int]:
    assists: dict[int, int] = {}
    for scorer, goals in goals_by_player.items():
        for _ in range(goals):
            candidates = [p for p in players if p != scorer]
            if not candidates:
                continue
            assister = rng.choice(candidates)
            assists[assister] = assists.get(assister, 0) + 1
    return assists


def _random_cards(players: list[int], rng: random.Random, card_type: str) -> dict[int, int]:
    count = rng.randint(0, 1 if card_type == "red" else 3)
    result: dict[int, int] = {}
    for _ in range(count):
        player = rng.choice(players) if players else None
        if player is None:
            continue
        result[player] = result.get(player, 0) + 1
    return result


def _events_for_team(
    match_id: int,
    team_id: int,
    goals_by_player: dict[int, int],
    yellows: dict[int, int],
    reds: dict[int, int],
    bench: list[int],
    rng: random.Random,
) -> list[tuple]:
    events: list[tuple] = []
    for player_id, goal_count in goals_by_player.items():
        for _ in range(goal_count):
            minute = rng.randint(5, 90)
            events.append((match_id, minute, team_id, player_id, "goal", ""))
    for player_id, count in yellows.items():
        for _ in range(count):
            events.append((match_id, rng.randint(10, 88), team_id, player_id, "yellow", ""))
    for player_id, count in reds.items():
        for _ in range(count):
            events.append((match_id, rng.randint(30, 90), team_id, player_id, "red", ""))
    for player_id in bench:
        minute = rng.randint(55, 75)
        events.append((match_id, minute, team_id, player_id, "sub_on", ""))
    return events


def _player_stat_rows(
    match_id: int,
    team_id: int,
    lineup: list[int],
    bench: list[int],
    goals: dict[int, int],
    assists: dict[int, int],
    yellows: dict[int, int],
    reds: dict[int, int],
    team_xg: float,
    rng: random.Random,
) -> list[tuple]:
    rows: list[tuple] = []
    total_players = len(lineup) + len(bench)
    per_player_xg = team_xg / total_players if total_players else 0
    for idx, player_id in enumerate(lineup):
        minutes = rng.randint(70, 95)
        shots = goals.get(player_id, 0) + rng.randint(0, 3)
        shots_on = max(goals.get(player_id, 0), rng.randint(0, shots))
        rows.append(
            (
                match_id,
                player_id,
                team_id,
                minutes,
                goals.get(player_id, 0),
                assists.get(player_id, 0),
                shots,
                shots_on,
                round(per_player_xg + rng.uniform(0, 0.3), 2),
                round((assists.get(player_id, 0) or 0) * 0.15, 2),
                yellows.get(player_id, 0),
                reds.get(player_id, 0),
                1,
            )
        )
    for player_id in bench:
        minutes = rng.randint(10, 35)
        shots = goals.get(player_id, 0) + rng.randint(0, 2)
        shots_on = max(goals.get(player_id, 0), rng.randint(0, shots))
        rows.append(
            (
                match_id,
                player_id,
                team_id,
                minutes,
                goals.get(player_id, 0),
                assists.get(player_id, 0),
                shots,
                shots_on,
                round(per_player_xg * 0.8, 2),
                round((assists.get(player_id, 0) or 0) * 0.12, 2),
                yellows.get(player_id, 0),
                reds.get(player_id, 0),
                0,
            )
        )
    return rows


def _persist_events(conn: sqlite3.Connection, events: list[tuple]):
    conn.executemany(
        "INSERT INTO match_events(match_id, minute, team_id, player_id, type, detail) VALUES (?, ?, ?, ?, ?, ?)",
        events,
    )
    conn.commit()


def _persist_player_stats(conn: sqlite3.Connection, stats: list[tuple]):
    conn.executemany(
        """
        INSERT INTO player_match_stats(
            match_id, player_id, team_id, minutes, goals, assists, shots, shots_on_target,
            xg, xa, yellow, red, starts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        stats,
    )
    conn.commit()


def _build_and_store_standings(
    conn: sqlite3.Connection, season: int, matches: list[MatchResult]
):
    team_names = {row["id"]: row["name"] for row in conn.execute("SELECT id, name FROM teams")}
    standings_data: dict[tuple[int, str], dict[int, dict]] = {}
    for stage in ["apertura", "clausura", "intermedio"]:
        standings_data[(season, stage)] = {
            team_id: {"mp": 0, "w": 0, "d": 0, "l": 0, "gf": 0, "ga": 0}
            for team_id in team_names
        }

    for m in matches:
        key = (m.season_year, m.stage_code)
        if key not in standings_data:
            standings_data[key] = {
                team_id: {"mp": 0, "w": 0, "d": 0, "l": 0, "gf": 0, "ga": 0}
                for team_id in team_names
            }
        table = standings_data[key]
        for team_id, gf, ga, is_home in [
            (m.home_team_id, m.home_goals, m.away_goals, True),
            (m.away_team_id, m.away_goals, m.home_goals, False),
        ]:
            row = table[team_id]
            row["mp"] += 1
            row["gf"] += gf
            row["ga"] += ga
            if gf > ga:
                row["w"] += 1
            elif gf == ga:
                row["d"] += 1
            else:
                row["l"] += 1

    # annual table aggregates apertura + clausura
    standings_data[(season, "anual")] = {
        team_id: {"mp": 0, "w": 0, "d": 0, "l": 0, "gf": 0, "ga": 0}
        for team_id in team_names
    }
    for stage in ["apertura", "clausura"]:
        table = standings_data[(season, stage)]
        annual = standings_data[(season, "anual")]
        for team_id, stats in table.items():
            target = annual[team_id]
            for key in ["mp", "w", "d", "l", "gf", "ga"]:
                target[key] += stats[key]

    conn.executemany("DELETE FROM standings WHERE season_year=?", [(season,)])
    for (season_year, stage_code), table in standings_data.items():
        last5 = _last5_strings(conn, season_year, stage_code)
        top_scorers = _top_scorer_by_team(conn, season_year, stage_code)
        attendances = _avg_attendance_by_team(conn, season_year, stage_code)
        gk_map = _goalkeeper_map(conn)
        rows = []
        for team_id, stats in table.items():
            gd = stats["gf"] - stats["ga"]
            pts = stats["w"] * 3 + stats["d"]
            mp = stats["mp"] or 1
            ppg = round(pts / mp, 2)
            rows.append(
                (
                    season_year,
                    stage_code,
                    team_id,
                    stats["mp"],
                    stats["w"],
                    stats["d"],
                    stats["l"],
                    stats["gf"],
                    stats["ga"],
                    gd,
                    pts,
                    ppg,
                    last5.get(team_id, ""),
                    top_scorers.get(team_id, ""),
                    gk_map.get(team_id, ""),
                    attendances.get(team_id, 0),
                )
            )
        rows.sort(key=lambda r: (-r[10], -r[9], -r[7], team_names.get(r[2], "")))
        conn.executemany(
            """
            INSERT INTO standings(
                season_year, stage_code, team_id, mp, w, d, l, gf, ga, gd, pts, ppg, last5,
                top_scorer, goalkeeper, avg_attendance
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    conn.commit()


def _goalkeeper_map(conn: sqlite3.Connection) -> dict[int, str]:
    rows = conn.execute(
        "SELECT team_id, full_name FROM players WHERE position = 'GK' GROUP BY team_id"
    ).fetchall()
    return {row["team_id"]: row["full_name"] for row in rows}


def _last5_strings(conn: sqlite3.Connection, season: int, stage: str) -> dict[int, str]:
    rows = conn.execute(
        """
        SELECT home_team_id, away_team_id, home_goals, away_goals
        FROM matches
        WHERE season_year = ? AND (stage_code = ? OR (? = 'anual' AND stage_code IN ('apertura','clausura')))
        ORDER BY date DESC, id DESC
        """,
        (season, stage, stage),
    ).fetchall()
    history: dict[int, list[str]] = {}
    for match in rows:
        home, away, hg, ag = match
        if hg > ag:
            home_res, away_res = "W", "L"
        elif hg == ag:
            home_res = away_res = "D"
        else:
            home_res, away_res = "L", "W"
        history.setdefault(home, []).append(home_res)
        history.setdefault(away, []).append(away_res)
    return {team: "".join(results[:5]) for team, results in history.items()}


def _top_scorer_by_team(conn: sqlite3.Connection, season: int, stage: str) -> dict[int, str]:
    rows = conn.execute(
        """
        SELECT t.id as team_id, p.full_name, COUNT(*) as goals
        FROM match_events e
        JOIN matches m ON e.match_id = m.id
        JOIN players p ON e.player_id = p.id
        JOIN teams t ON p.team_id = t.id
        WHERE m.season_year = ? AND (m.stage_code = ? OR (? = 'anual' AND m.stage_code IN ('apertura','clausura')))
              AND e.type = 'goal'
        GROUP BY t.id, p.id
        ORDER BY goals DESC
        """,
        (season, stage, stage),
    ).fetchall()
    result: dict[int, str] = {}
    for row in rows:
        if row["team_id"] not in result:
            result[row["team_id"]] = f"{row['full_name']}–{row['goals']}"
    return result


def _avg_attendance_by_team(conn: sqlite3.Connection, season: int, stage: str) -> dict[int, float]:
    rows = conn.execute(
        """
        SELECT home_team_id, AVG(attendance) as avg_att
        FROM matches
        WHERE season_year = ? AND (stage_code = ? OR (? = 'anual' AND stage_code IN ('apertura','clausura')))
        GROUP BY home_team_id
        """,
        (season, stage, stage),
    ).fetchall()
    return {row["home_team_id"]: round(row["avg_att"], 2) for row in rows}


def _primary_gk_map(conn: sqlite3.Connection, season: int, stage: str) -> dict[int, dict]:
    stages = _stages_for_query(stage)
    placeholders = ",".join("?" for _ in stages)
    rows = conn.execute(
        f"""
        SELECT p.team_id, p.id as player_id, p.full_name, SUM(ps.minutes) as mins
        FROM player_match_stats ps
        JOIN matches m ON ps.match_id = m.id
        JOIN players p ON ps.player_id = p.id
        WHERE m.season_year = ? AND m.stage_code IN ({placeholders}) AND p.position = 'GK'
        GROUP BY p.team_id, p.id
        ORDER BY mins DESC
        """,
        (season, *stages),
    ).fetchall()
    result: dict[int, dict] = {}
    for row in rows:
        if row["team_id"] not in result:
            result[row["team_id"]] = {
                "player_id": row["player_id"],
                "name": row["full_name"],
                "minutes": row["mins"],
            }
    return result


def _top_scorer_struct_by_team(
    conn: sqlite3.Connection, season: int, stage: str
) -> dict[int, dict]:
    stages = _stages_for_query(stage)
    placeholders = ",".join("?" for _ in stages)
    rows = conn.execute(
        f"""
        SELECT t.id as team_id, p.id as player_id, p.full_name, COUNT(*) as goals
        FROM match_events e
        JOIN matches m ON e.match_id = m.id
        JOIN players p ON e.player_id = p.id
        JOIN teams t ON p.team_id = t.id
        WHERE m.season_year = ? AND m.stage_code IN ({placeholders}) AND e.type = 'goal'
        GROUP BY t.id, p.id
        ORDER BY goals DESC
        """,
        (season, *stages),
    ).fetchall()
    result: dict[int, dict] = {}
    for row in rows:
        if row["team_id"] not in result:
            result[row["team_id"]] = {
                "player_id": row["player_id"],
                "name": row["full_name"],
                "goals": row["goals"],
            }
    return result


# ---- Query helpers ----

def get_metadata(conn: Optional[sqlite3.Connection] = None):
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True
    try:
        seasons = [row[0] for row in conn.execute("SELECT year FROM seasons ORDER BY year")]
        stages = [row[0] for row in conn.execute("SELECT code FROM stages ORDER BY code")]
        teams = [
            {
                "id": row["id"],
                "name": row["name"],
                "logo_key": row["logo_key"],
                "short_name": row["short_name"],
            }
            for row in conn.execute("SELECT id, name, logo_key, short_name FROM teams ORDER BY name")
        ]
        return {
            "seasons": seasons,
            "stages": stages,
            "default_season": DEFAULT_SEASON,
            "default_stage": DEFAULT_STAGE,
            "teams": teams,
        }
    finally:
        if close_conn:
            conn.close()


def _stages_for_query(stage: str) -> List[str]:
    if stage == "anual":
        return ["apertura", "clausura"]
    return [stage]


def compute_table(season: int, stage: str) -> dict:
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT s.*, t.name as team, t.logo_key
            FROM standings s
            JOIN teams t ON s.team_id = t.id
            WHERE s.season_year = ? AND s.stage_code = ?
            ORDER BY pts DESC, gd DESC, gf DESC, team ASC
            """,
            (season, stage),
        ).fetchall()
        normalized = [
            {
                "pos": idx,
                "team": row["team"],
                "team_id": row["team_id"],
                "logo_key": row["logo_key"],
                "mp": row["mp"],
                "w": row["w"],
                "d": row["d"],
                "l": row["l"],
                "gf": row["gf"],
                "ga": row["ga"],
                "gc": row["ga"],
                "gd": row["gd"],
                "pts": row["pts"],
                "ppg": row["ppg"],
                "last5": row["last5"],
                "avg_attendance": row["avg_attendance"],
                # backward compatible keys
                "pj": row["mp"],
                "pg": row["w"],
                "pe": row["d"],
                "pp": row["l"],
                "dg": row["gd"],
            }
            for idx, row in enumerate(rows, start=1)
        ]
        return {
            "season": season,
            "stage": stage,
            "rows": normalized,
            "updated_at": datetime.utcnow().isoformat(),
            "source": "seed",
        }
    finally:
        conn.close()


def get_matches(season: int, stage: str) -> list[sqlite3.Row]:
    conn = get_connection()
    try:
        return get_matches_for_conn(conn, season, stage)
    finally:
        conn.close()


def get_matches_for_conn(conn: sqlite3.Connection, season: int, stage: str) -> list[sqlite3.Row]:
    stages = _stages_for_query(stage)
    placeholders = ",".join("?" for _ in stages)
    return conn.execute(
        f"SELECT * FROM matches WHERE season_year = ? AND stage_code IN ({placeholders}) ORDER BY date, id",
        (season, *stages),
    ).fetchall()


def list_fixtures(
    season: int, stage: str, team_id: Optional[int] = None, round_number: Optional[str] = None
) -> list[dict]:
    conn = get_connection()
    try:
        matches = get_matches_for_conn(conn, season, stage)
        teams = {
            row["id"]: {"name": row["name"], "logo_key": row["logo_key"]}
            for row in conn.execute("SELECT id, name, logo_key FROM teams")
        }
        round_filter = str(round_number) if round_number is not None else None
        filtered = []
        for match in matches:
            if team_id and match["home_team_id"] != team_id and match["away_team_id"] != team_id:
                continue
            if round_filter and str(match["round"]) != round_filter:
                continue
            filtered.append(
                {
                    "match_id": match["id"],
                    "date": match["date"],
                    "time": match["time"],
                    "round": match["round"],
                    "home": teams.get(match["home_team_id"], {}).get("name", ""),
                    "away": teams.get(match["away_team_id"], {}).get("name", ""),
                    "home_team_id": match["home_team_id"],
                    "away_team_id": match["away_team_id"],
                    "home_logo_key": teams.get(match["home_team_id"], {}).get("logo_key", ""),
                    "away_logo_key": teams.get(match["away_team_id"], {}).get("logo_key", ""),
                    "home_goals": match["home_goals"],
                    "away_goals": match["away_goals"],
                    "attendance": match["attendance"],
                    "venue": match["venue"],
                    "referee": match["referee"],
                }
            )
        return filtered
    finally:
        conn.close()


def list_match_events(match_id: int) -> list[dict]:
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT e.minute, e.team_id, t.name as team, e.player_id, p.full_name as player, e.type, e.detail
            FROM match_events e
            JOIN teams t ON e.team_id = t.id
            LEFT JOIN players p ON e.player_id = p.id
            WHERE e.match_id = ?
            ORDER BY e.minute ASC, e.id ASC
            """,
            (match_id,),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def list_scorers(season: int, stage: str, top: int = 20) -> list[dict]:
    conn = get_connection()
    try:
        stages = _stages_for_query(stage)
        placeholders = ",".join("?" for _ in stages)
        rows = conn.execute(
            f"""
            SELECT p.id as player_id, p.full_name as player, t.id as team_id, t.name as team, COUNT(*) as goals
            FROM match_events e
            JOIN matches m ON e.match_id = m.id
            JOIN players p ON e.player_id = p.id
            JOIN teams t ON p.team_id = t.id
            WHERE m.season_year = ? AND m.stage_code IN ({placeholders}) AND e.type = 'goal'
            GROUP BY p.id, t.name
            ORDER BY goals DESC, player ASC
            LIMIT ?
            """,
            (season, *stages, top),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def cards_by_team(conn: sqlite3.Connection, season: int, stage: str) -> list[dict]:
    stages = _stages_for_query(stage)
    placeholders = ",".join("?" for _ in stages)
    rows = conn.execute(
        f"""
        SELECT t.id as team_id,
               t.name as team,
               t.logo_key as logo_key,
               SUM(CASE WHEN e.type = 'yellow' THEN 1 ELSE 0 END) as yellow,
               SUM(CASE WHEN e.type = 'red' THEN 1 ELSE 0 END) as red
        FROM match_events e
        JOIN matches m ON e.match_id = m.id
        JOIN teams t ON e.team_id = t.id
        WHERE m.season_year = ? AND m.stage_code IN ({placeholders})
        GROUP BY t.id, t.name, t.logo_key
        ORDER BY t.name
        """,
        (season, *stages),
    ).fetchall()
    return [dict(row) for row in rows]


def stats_insights(season: int, stage: str) -> dict:
    table = compute_table(season, stage)
    conn = get_connection()
    try:
        team_lookup = {
            row["id"]: {"name": row["name"], "logo_key": row["logo_key"]}
            for row in conn.execute("SELECT id, name, logo_key FROM teams")
        }
        attendance = _avg_attendance_by_team(conn, season, stage)
        return {
            "season": season,
            "stage": stage,
            "goals_for_by_team": [
                {
                    "team_id": row["team_id"],
                    "team": row["team"],
                    "value": row["gf"],
                    "logo_key": row["logo_key"],
                }
                for row in table["rows"]
            ],
            "points_by_team": [
                {
                    "team_id": row["team_id"],
                    "team": row["team"],
                    "value": row["pts"],
                    "logo_key": row["logo_key"],
                }
                for row in table["rows"]
            ],
            "cards_by_team": cards_by_team(conn, season, stage),
            "attendance_by_team": [
                {
                    "team_id": team_id,
                    "team": meta.get("name", ""),
                    "value": attendance.get(team_id, 0),
                    "logo_key": meta.get("logo_key", ""),
                }
                for team_id, meta in team_lookup.items()
            ],
            "top_scorers": list_scorers(season, stage, top=10),
            "source": "seed",
        }
    finally:
        conn.close()


def list_teams_basic() -> list[dict]:
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT id, name, logo_key FROM teams ORDER BY name"
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def teams_summary(season: int, stage: str) -> list[dict]:
    conn = get_connection()
    try:
        attendance = _avg_attendance_by_team(conn, season, stage)
        gk_map = _primary_gk_map(conn, season, stage)
        top_map = _top_scorer_struct_by_team(conn, season, stage)
        teams = conn.execute(
            "SELECT id, name, logo_key FROM teams ORDER BY name"
        ).fetchall()
        return [
            {
                "team_id": row["id"],
                "team": row["name"],
                "logo_key": row["logo_key"],
                "avg_attendance": attendance.get(row["id"], 0),
                "primary_gk": gk_map.get(row["id"]),
                "top_scorer": top_map.get(row["id"]),
            }
            for row in teams
        ]
    finally:
        conn.close()


def players_overview(
    season: int, stage: str, team_id: Optional[int] = None
) -> list[dict]:
    stats = player_standard_stats(season, stage, team_id=team_id)
    return [
        {
            "player_id": row["player_id"],
            "full_name": row["player"],
            "team": row["team"],
            "team_id": row["team_id"],
            "position": row["pos"],
            "nationality_iso2": row.get("nationality_iso2", ""),
            "mp": row.get("mp", 0),
            "starts": row.get("starts", 0),
            "goals": row["gls"],
            "assists": row["ast"],
            "minutes": row["min"],
            "min": row.get("min", 0),
            "shots": row.get("shots", row.get("sh", 0)),
            "shots_on_target": row.get("shots_on_target", row.get("sot", 0)),
            "yellows": row["crdy"],
            "reds": row["crdr"],
            "xg": row["xg"],
            "xa": row["xa"],
        }
        for row in stats
    ]


def teams_list() -> list[str]:
    conn = get_connection()
    try:
        rows = conn.execute("SELECT name FROM teams ORDER BY name").fetchall()
        return [row[0] for row in rows]
    finally:
        conn.close()


def discipline_table(season: int, stage: str) -> list[dict]:
    conn = get_connection()
    try:
        stats = cards_by_team(conn, season, stage)
        match_counts = conn.execute(
            """
            SELECT t.name as team, COUNT(*) as pj
            FROM matches m
            JOIN teams t ON m.home_team_id = t.id OR m.away_team_id = t.id
            WHERE m.season_year = ? AND m.stage_code IN (%s)
            GROUP BY t.name
            """
            % ",".join("?" for _ in _stages_for_query(stage)),
            (season, *_stages_for_query(stage)),
        ).fetchall()
        matches_map = {row["team"]: row["pj"] for row in match_counts}
        enriched = []
        for row in stats:
            pj = matches_map.get(row["team"], 0)
            total = row["yellow"] + row["red"]
            per_game = round(total / pj, 2) if pj else 0
            enriched.append(
                {
                    "Equipo": row["team"],
                    "PJ": pj,
                    "Amarillas": row["yellow"],
                    "Rojas": row["red"],
                    "Tarjetas totales": total,
                    "Tarjetas/partido": per_game,
                }
            )
        return enriched
    finally:
        conn.close()


def summary_for_team(conn: sqlite3.Connection, season: int, stage: str, team: str) -> dict:
    table = compute_table(season, stage)
    row = next((r for r in table["rows"] if r["team"] == team), None)
    if not row:
        return {}
    cards = {c["team"]: c for c in cards_by_team(conn, season, stage)}
    card_row = cards.get(team, {"yellow": 0, "red": 0})
    return {
        "team": team,
        "pts": row["pts"],
        "dg": row["dg"],
        "gf": row["gf"],
        "gc": row.get("ga", row.get("gc", 0)),
        "amarillas": card_row.get("yellow", 0),
        "rojas": card_row.get("red", 0),
        "last5": row.get("last5", ""),
    }


def consultor_advice(team_a: str, team_b: str, season: int, stage: str) -> dict:
    conn = get_connection()
    try:
        meta = get_metadata(conn)
        valid_names = {t["name"] for t in meta["teams"]}
        if team_a not in valid_names or team_b not in valid_names:
            raise ValueError("Equipo no válido")
        summary_a = summary_for_team(conn, season, stage, team_a)
        summary_b = summary_for_team(conn, season, stage, team_b)
        recommendation = _build_recommendation(summary_a, summary_b)
        return {
            "season": season,
            "stage": stage,
            "equipo_a": summary_a,
            "equipo_b": summary_b,
            "recomendacion": recommendation,
        }
    finally:
        conn.close()


def _build_recommendation(a: dict, b: dict) -> str:
    if not a or not b:
        return "No hay datos suficientes para generar la recomendación."
    score_a = a["pts"] * 2 + a["dg"]
    score_b = b["pts"] * 2 + b["dg"]
    diff = score_a - score_b
    if abs(diff) < 3:
        base = "Se espera un duelo parejo."
    elif diff > 0:
        base = f"{a['team']} llega mejor posicionado que {b['team']}."
    else:
        base = f"{b['team']} llega mejor posicionado que {a['team']}."
    discipline_hint = ""
    if a["amarillas"] + a["rojas"] > b["amarillas"] + b["rojas"]:
        discipline_hint = f" {a['team']} debe cuidar la disciplina."
    elif b["amarillas"] + b["rojas"] > a["amarillas"] + a["rojas"]:
        discipline_hint = f" {b['team']} debe cuidar la disciplina."
    form_hint = ""
    if a.get("last5") and b.get("last5"):
        form_hint = f" Rachas: {a['team']} {a['last5']} vs {b['team']} {b['last5']}."
    return base + discipline_hint + form_hint


def player_standard_stats(
    season: int, stage: str, team_id: Optional[int] = None
) -> list[dict]:
    conn = get_connection()
    try:
        stages = _stages_for_query(stage)
        placeholders = ",".join("?" for _ in stages)
        team_filter = ""
        params: list = [season, *stages]
        if team_id:
            team_filter = "AND p.team_id = ?"
            params.append(team_id)
        rows = conn.execute(
            f"""
            SELECT p.id as player_id, p.full_name, p.position, p.nationality, p.birth_year,
                   t.name as team, t.id as team_id,
                   SUM(ps.starts) as starts,
                   COUNT(*) as mp,
                   SUM(ps.minutes) as minutes,
                   SUM(ps.goals) as goals,
                   SUM(ps.assists) as assists,
                   SUM(ps.shots) as shots,
                   SUM(ps.shots_on_target) as shots_on_target,
                   SUM(ps.xg) as xg,
                   SUM(ps.xa) as xa,
                   SUM(ps.yellow) as yellow,
                   SUM(ps.red) as red
            FROM player_match_stats ps
            JOIN matches m ON ps.match_id = m.id
            JOIN players p ON ps.player_id = p.id
            JOIN teams t ON p.team_id = t.id
            WHERE m.season_year = ? AND m.stage_code IN ({placeholders}) {team_filter}
            GROUP BY p.id, t.id
            ORDER BY goals DESC, assists DESC, minutes DESC
            """,
            params,
        ).fetchall()
        result = []
        for row in rows:
            age = None
            if row["birth_year"]:
                age = season - row["birth_year"]
            nat = row["nationality"] or ""
            result.append(
                {
                    "player_id": row["player_id"],
                    "player": row["full_name"],
                    "team": row["team"],
                    "team_id": row["team_id"],
                    "nation": nat,
                    "nationality_iso2": nat,
                    "pos": row["position"] or "MF",
                    "age": age,
                    "mp": row["mp"],
                    "starts": row["starts"],
                    "min": row["minutes"],
                    "gls": row["goals"],
                    "ast": row["assists"],
                    "sh": row["shots"],
                    "sot": row["shots_on_target"],
                    "crdy": row["yellow"],
                    "crdr": row["red"],
                    "xg": round(row["xg"], 2),
                    "xa": round(row["xa"], 2),
                }
            )
        return result
    finally:
        conn.close()


def reseed_database(hard: bool = False):
    if hard:
        DB_PATH.unlink(missing_ok=True)
    ensure_db()

