from pathlib import Path
import csv

from Modelo.equipo import Equipo
from Modelo.stats import EquipoTemporadaStats
from Modelo.torneo import Torneo


# Ruta al CSV que generaste con el scraper
CSV_PATH = Path("Data") / "standings_uruguay.csv"


def elegir_columna(row: dict, posibles_nombres: list[str]) -> str:
    """
    Devuelve row[columna] usando el primer nombre que exista en el CSV.
    Si ninguno existe, tira KeyError con info para depurar.
    """
    for nombre in posibles_nombres:
        if nombre in row:
            return row[nombre]
    raise KeyError(
        f"No encontré ninguna de las columnas: {posibles_nombres}\n"
        f"Columnas disponibles: {list(row.keys())}"
    )


def cargar_desde_csv(csv_path: Path, temporada: int) -> tuple[Torneo, list[EquipoTemporadaStats]]:
    """
    Lee el CSV de standings y crea:
    - un objeto Torneo
    - una lista de EquipoTemporadaStats
    """
    torneo = Torneo(nombre="Primera División Uruguay", temporada=temporada)

    equipos_por_nombre: dict[str, Equipo] = {}
    stats_por_equipo: list[EquipoTemporadaStats] = []

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        # Definimos las columnas manualmente porque el CSV no tiene encabezados
        fieldnames = [
            "Pos", "Equipo", "PJ", "PG", "PE", "PP",
            "GF", "GC", "DG", "Puntos", "Promedio", "Forma",
            "Asistencia", "Goleador", "Arquero", "Extra"
        ]
        reader = csv.DictReader(f, fieldnames=fieldnames)
        print("Columnas detectadas en el CSV:")
        print(reader.fieldnames)

        contador_id = 1

        for row in reader:
            # Ajustá estos posibles nombres si hiciera falta,
            # según lo que viste en el CSV.
            nombre_equipo = elegir_columna(row, ["Squad", "Equipo", "Team"])

            mp = int(elegir_columna(row, ["MP", "J", "PJ"]))
            w = int(elegir_columna(row, ["W", "G", "PG"]))
            d = int(elegir_columna(row, ["D", "E", "PE"]))
            l = int(elegir_columna(row, ["L", "P", "PP"]))

            gf = int(elegir_columna(row, ["GF", "Goles a favor"]))
            ga = int(elegir_columna(row, ["GA", "GC", "Goles en contra"]))
            pts = int(elegir_columna(row, ["Pts", "Puntos"]))

            # Reutilizamos Equipo si ya existe
            if nombre_equipo not in equipos_por_nombre:
                equipos_por_nombre[nombre_equipo] = Equipo(
                    id_equipo=contador_id,
                    nombre_oficial=nombre_equipo,
                    alias=[nombre_equipo],
                )
                contador_id += 1

            equipo = equipos_por_nombre[nombre_equipo]

            stats = EquipoTemporadaStats(
                equipo=equipo,
                temporada=temporada,
                torneo="Anual",
                partidos_jugados=mp,
                ganados=w,
                empatados=d,
                perdidos=l,
                goles_favor=gf,
                goles_contra=ga,
                puntos=pts,
            )

            stats_por_equipo.append(stats)
            torneo.agregar_equipo(equipo)

    return torneo, stats_por_equipo


def mostrar_resumen(torneo: Torneo, stats: list[EquipoTemporadaStats]) -> None:
    print("\n=== Resumen Torneo ===")
    print(torneo.resumen_general())

    # Top 3 por puntos
    print("=== Top 3 equipos por puntos ===")
    top3 = sorted(stats, key=lambda s: s.puntos, reverse=True)[:3]
    for pos, fila in enumerate(top3, start=1):
        print(
            f"{pos}. {fila.equipo.nombre_oficial} - "
            f"{fila.puntos} pts, DG {fila.diferencia_goles()} "
            f"({fila.goles_favor} GF / {fila.goles_contra} GC)"
        )

    # Mejor ataque
    mejor_ataque = max(stats, key=lambda s: s.goles_favor)
    print("\nEquipo con más goles a favor:")
    print(f"- {mejor_ataque.equipo.nombre_oficial}: {mejor_ataque.goles_favor} GF")

    # Mejor defensa
    mejor_defensa = min(stats, key=lambda s: s.goles_contra)
    print("\nEquipo con menos goles en contra:")
    print(f"- {mejor_defensa.equipo.nombre_oficial}: {mejor_defensa.goles_contra} GC")

    print("\nDemostración Sprint 2 completa ✅")


def main():
    if not CSV_PATH.exists():
        print(f"No encontré el archivo {CSV_PATH}. "
              f"Asegurate de copiar standings_uruguay.csv a la carpeta Data.")
        return

    # Por ahora asumimos temporada 2024; se puede generalizar después
    torneo, stats = cargar_desde_csv(CSV_PATH, temporada=2024)
    mostrar_resumen(torneo, stats)


if __name__ == "__main__":
    main()
