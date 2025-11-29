# modelo/jugador.py

from dataclasses import dataclass


@dataclass
class Jugador:
    nombre: str
    equipo: str
    goles: int = 0
    asistencias: int = 0
    minutos_jugados: int = 0
    titular: bool = True

    def promedio_goles_por_partido(self, partidos_jugados: int) -> float:
        """Calcula el promedio de goles por partido."""
        if partidos_jugados == 0:
            return 0.0
        return round(self.goles / partidos_jugados, 2)

    def eficiencia_ofensiva(self) -> float:
        """
        Devuelve una métrica simple de impacto ofensivo,
        considerando goles y asistencias ponderados.
        """
        return self.goles * 1.5 + self.asistencias

    def __str__(self) -> str:
        tipo = "Titular" if self.titular else "Suplente"
        return f"{self.nombre} ({self.equipo}) - {tipo}"
