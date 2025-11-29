# modelo/torneo.py

from dataclasses import dataclass, field
from typing import List
from .equipo import Equipo
from .jugador import Jugador


@dataclass
class Torneo:
    nombre: str
    temporada: int
    equipos: List[Equipo] = field(default_factory=list)
    jugadores: List[Jugador] = field(default_factory=list)

    def agregar_equipo(self, equipo: Equipo) -> None:
        """Agrega un equipo al torneo si no existe aún."""
        if equipo.nombre_oficial not in [e.nombre_oficial for e in self.equipos]:
            self.equipos.append(equipo)

    def agregar_jugador(self, jugador: Jugador) -> None:
        """Agrega un jugador al torneo."""
        self.jugadores.append(jugador)

    def buscar_equipo(self, nombre: str) -> Equipo | None:
        """Devuelve un equipo por nombre o alias."""
        for e in self.equipos:
            if e.coincide_nombre(nombre):
                return e
        return None

    def ranking_equipos(self) -> List[str]:
        """
        Devuelve un ranking básico basado en los goles de los jugadores
        registrados (puede ser mejorado al cargar estadísticas reales).
        """
        ranking = {}
        for j in self.jugadores:
            ranking[j.equipo] = ranking.get(j.equipo, 0) + j.goles
        return sorted(ranking.items(), key=lambda x: x[1], reverse=True)

    def goleadores_top(self, n: int = 5) -> List[Jugador]:
        """Devuelve los n jugadores con más goles."""
        return sorted(self.jugadores, key=lambda j: j.goles, reverse=True)[:n]

    def resumen_general(self) -> str:
        """Devuelve un resumen general del torneo."""
        return (
            f"Torneo {self.nombre} {self.temporada}\n"
            f"Equipos registrados: {len(self.equipos)}\n"
            f"Jugadores registrados: {len(self.jugadores)}\n"
        )
# modelo/torneo.py

from dataclasses import dataclass, field
from typing import List
from Modelo.equipo import Equipo
from Modelo.jugador import Jugador


@dataclass
class Torneo:
    nombre: str
    temporada: int
    equipos: List[Equipo] = field(default_factory=list)
    jugadores: List[Jugador] = field(default_factory=list)

    def agregar_equipo(self, equipo: Equipo) -> None:
        """Agrega un equipo al torneo si no existe aún."""
        if equipo.nombre_oficial not in [e.nombre_oficial for e in self.equipos]:
            self.equipos.append(equipo)

    def agregar_jugador(self, jugador: Jugador) -> None:
        """Agrega un jugador al torneo."""
        self.jugadores.append(jugador)

    def buscar_equipo(self, nombre: str) -> Equipo | None:
        """Devuelve un equipo por nombre o alias."""
        for e in self.equipos:
            if e.coincide_nombre(nombre):
                return e
        return None

    def ranking_equipos(self) -> List[str]:
        """
        Devuelve un ranking básico basado en los goles de los jugadores
        registrados (puede ser mejorado al cargar estadísticas reales).
        """
        ranking = {}
        for j in self.jugadores:
            ranking[j.equipo] = ranking.get(j.equipo, 0) + j.goles
        return sorted(ranking.items(), key=lambda x: x[1], reverse=True)

    def goleadores_top(self, n: int = 5) -> List[Jugador]:
        """Devuelve los n jugadores con más goles."""
        return sorted(self.jugadores, key=lambda j: j.goles, reverse=True)[:n]

    def resumen_general(self) -> str:
        """Devuelve un resumen general del torneo."""
        return (
            f"Torneo {self.nombre} {self.temporada}\n"
            f"Equipos registrados: {len(self.equipos)}\n"
            f"Jugadores registrados: {len(self.jugadores)}\n"
        )
