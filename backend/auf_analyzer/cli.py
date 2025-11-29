"""Interfaz de línea de comandos para el Campeonato AUF Analyzer.

Comandos principales:
- ``fetch``: ejecuta la obtención y almacenamiento asincrónico.
- ``analyze``: genera estadísticas legibles por consola a partir de un CSV.
- ``pipeline``: combina ambos pasos para dejar todo actualizado en un solo paso.
"""

from __future__ import annotations

import argparse
import asyncio
from textwrap import dedent

from . import pipeline
from . import services


def _print_header(title: str) -> None:
    print("\n" + title)
    print("=" * len(title))


def _format_ranking(equipos: list[dict[str, object]], limit: int) -> str:
    rows = ["Pos Equipo                      Pts  DG  GF"]
    for idx, equipo in enumerate(equipos[:limit], start=1):
        rows.append(
            f"{idx:>3} {equipo['name']:<25} {equipo['pts']:>3}  {equipo['gd']:>3}  {equipo['gf']:>3}"
        )
    return "\n".join(rows)


def _format_scorers(goleadores: list[dict[str, object]], limit: int) -> str:
    rows = ["#  Jugador                      Equipo                  G"]
    for idx, g in enumerate(goleadores[:limit], start=1):
        rows.append(f"{idx:>2} {g['Jugador']:<27} {g['Equipo']:<22} {g['Goles']:>2}")
    return "\n".join(rows)


def _load_csv_path(stage_slug: str | None) -> Path | None:
    if stage_slug is None:
        return None
    try:
        return pipeline.stage_csv_path_from_slug(stage_slug)
    except KeyError as exc:
        raise SystemExit(str(exc))


def cmd_fetch(_: argparse.Namespace) -> None:
    summary = asyncio.run(pipeline.run_pipeline())
    _print_header("Resumen de descarga asincrónica")
    for stage in summary["stages"]:
        status = "OK" if stage["ok"] else f"ERROR: {stage['error']}"
        print(f"- {stage['label']} ({stage['season']}) -> {status}")


def cmd_analyze(args: argparse.Namespace) -> None:
    csv_path = _load_csv_path(args.stage)
    equipos = services.ranking_equipos_por_puntos(csv_path=csv_path)
    mejores = services.mejores_ataques(top=args.top_attacks, csv_path=csv_path)
    goleadores = services.top_scorers(limit=args.top_scorers, csv_path=csv_path)

    origen = csv_path if csv_path else services.CSV_STANDINGS
    _print_header(f"Análisis sobre {origen}")
    print(_format_ranking(equipos, limit=min(len(equipos), args.top_ranking)))

    _print_header("Mejores ataques")
    for equipo in mejores:
        print(
            f"- {equipo['name']} | GF: {equipo['gf']} | MP: {equipo['mp']} | Nickname: {equipo['nickname']}"
        )

    _print_header("Tabla de goleadores")
    print(_format_scorers(goleadores, limit=min(len(goleadores), args.top_scorers)))


def cmd_pipeline(args: argparse.Namespace) -> None:
    cmd_fetch(args)
    cmd_analyze(args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="auf-cli",
        description=dedent(
            """
            Ejecuta el flujo completo del Campeonato AUF Analyzer desde consola.

            Ejemplos:
              python -m auf_analyzer.cli fetch
              python -m auf_analyzer.cli analyze --stage apertura_2024
              python -m auf_analyzer.cli pipeline --top-ranking 10
            """
        ),
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch_parser = subparsers.add_parser(
        "fetch", help="Descarga asincrónicamente todas las tablas definidas en el pipeline"
    )
    fetch_parser.set_defaults(func=cmd_fetch)

    analyze_parser = subparsers.add_parser(
        "analyze", help="Calcula estadísticas a partir de un CSV concreto"
    )
    analyze_parser.add_argument(
        "--stage",
        choices=pipeline.available_stage_slugs(),
        help="Slug de la etapa a analizar (por defecto usa standings_uruguay.csv)",
    )
    analyze_parser.add_argument("--top-ranking", type=int, default=8)
    analyze_parser.add_argument("--top-attacks", type=int, default=5)
    analyze_parser.add_argument("--top-scorers", type=int, default=10)
    analyze_parser.set_defaults(func=cmd_analyze)

    pipeline_parser = subparsers.add_parser(
        "pipeline", help="Ejecuta fetch + analyze en un solo comando"
    )
    pipeline_parser.add_argument(
        "--stage",
        choices=pipeline.available_stage_slugs(),
        help="Analiza una etapa específica luego de la descarga",
    )
    pipeline_parser.add_argument("--top-ranking", type=int, default=8)
    pipeline_parser.add_argument("--top-attacks", type=int, default=5)
    pipeline_parser.add_argument("--top-scorers", type=int, default=10)
    pipeline_parser.set_defaults(func=cmd_pipeline)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":  # pragma: no cover
    main()
