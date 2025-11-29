# Modelo/stats.py

from __future__ import annotations
from dataclasses import dataclass


# Usamos strings en los type hints ("Equipo", "Jugador") para no tener que importar todavía.
# Esto evita problemas de imports circulares y de nombres de paquetes.


@dataclass
class EquipoTemporadaStats:
    """
    Estadísticas de un equipo en una temporada/torneo concreto.
    Ejemplo: Liverpool en la Anual 2024.
    """
    equipo: "Equipo"          # referencia a un objeto Equipo
    temporada: int            # año: 2024, 2025, etc.
    torneo: str               # "Apertura", "Clausura", "Intermedio", "Anual"

    partidos_jugados: int
    ganados: int
    empatados: int
    perdidos: int

    goles_favor: int
    goles_contra: int
    puntos: int

    def diferencia_goles(self) -> int:
        return self.goles_favor - self.goles_contra

    def puntos_por_partido(self) -> float:
        if self.partidos_jugados == 0:
            return 0.0
        return round(self.puntos / self.partidos_jugados, 2)

    def goles_por_partido(self) -> float:
        if self.partidos_jugados == 0:
            return 0.0
        return round(self.goles_favor / self.partidos_jugados, 2)

    def resumen(self) -> str:
        return (
            f"{self.equipo.nombre_oficial} - {self.torneo} {self.temporada}: "
            f"{self.puntos} pts, DG {self.diferencia_goles()} "
            f"({self.goles_favor} GF / {self.goles_contra} GC)"
        )


@dataclass
class JugadorTemporadaStats:
    """
    Estadísticas agregadas de un jugador en una temporada.
    Más adelante se puede separar por torneo si hace falta.
    """
    jugador: "Jugador"        # referencia a un objeto Jugador
    temporada: int
    partidos_jugados: int
    goles: int
    asistencias: int
    minutos_jugados: int

    def promedio_goles(self) -> float:
        if self.partidos_jugados == 0:
            return 0.0
        return round(self.goles / self.partidos_jugados, 2)

    def promedio_minutos(self) -> float:
        if self.partidos_jugados == 0:
            return 0.0
        return round(self.minutos_jugados / self.partidos_jugados, 1)

    def participacion_en_goles(self) -> float:
        """
        Mide cuánto participa en goles: goles + asistencias por partido.
        """
        if self.partidos_jugados == 0:
            return 0.0
        return round((self.goles + self.asistencias) / self.partidos_jugados, 2)

    def resumen(self) -> str:
        return (
            f"{self.jugador.nombre} {self.temporada}: "
            f"{self.goles} G, {self.asistencias} A en {self.partidos_jugados} partidos"
        )
