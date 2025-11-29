# modelo/partido.py

from dataclasses import dataclass, field
from typing import List, Optional
from .equipo import Equipo
from .jugador import Jugador


@dataclass
class EventoPartido:
    minuto: int
    equipo: Equipo
    jugador: Optional[Jugador] = None
    tipo: str = "gol"  # puede ser 'gol', 'tarjeta', 'cambio'
    detalle: str = ""  # por ejemplo "gol de cabeza", "penal", "amarilla"


@dataclass
class Partido:
    id_partido: str
    fecha: str
    local: Equipo
    visitante: Equipo
    goles_local: int
    goles_visitante: int
    eventos: List[EventoPartido] = field(default_factory=list)

    def resultado(self) -> str:
        """Devuelve el resultado en formato texto."""
        return f"{self.local.nombre_oficial} {self.goles_local} - {self.goles_visitante} {self.visitante.nombre_oficial}"

    def ganador(self) -> Optional[Equipo]:
        """Devuelve el equipo ganador (o None si fue empate)."""
        if self.goles_local > self.goles_visitante:
            return self.local
        elif self.goles_visitante > self.goles_local:
            return self.visitante
        return None

    def es_empate(self) -> bool:
        return self.goles_local == self.goles_visitante

    def goles_totales(self) -> int:
        """Devuelve la suma de goles del partido."""
        return self.goles_local + self.goles_visitante

    def agregar_evento(self, evento: EventoPartido) -> None:
        """Agrega un evento (gol, tarjeta, cambio) al registro del partido."""
        self.eventos.append(evento)

    def goles_por_tramo(self, inicio: int, fin: int) -> int:
        """Cuenta goles ocurridos entre los minutos 'inicio' y 'fin'."""
        return sum(1 for e in self.eventos if e.tipo == "gol" and inicio <= e.minuto <= fin)

    def goles_en_ultimos_15(self) -> int:
        """Cuenta los goles anotados entre el minuto 75 y el final."""
        return self.goles_por_tramo(75, 120)
