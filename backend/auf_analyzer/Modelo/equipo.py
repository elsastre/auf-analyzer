# modelo/equipo.py

from dataclasses import dataclass, field
import re
from typing import List


@dataclass
class Equipo:
    id_equipo: int
    nombre_oficial: str
    alias: List[str] = field(default_factory=list)
    ciudad: str | None = None

    def __post_init__(self):
        # Siempre incluimos el nombre oficial como alias
        if self.nombre_oficial not in self.alias:
            self.alias.append(self.nombre_oficial)

    def coincide_nombre(self, texto: str) -> bool:
        """
        Devuelve True si el nombre del equipo coincide con algún alias
        usando una comparación flexible / regex simple.
        """
        texto_norm = texto.lower()
        for a in self.alias:
            patron = re.escape(a.lower())
            if re.search(patron, texto_norm):
                return True
        return False

    def __str__(self) -> str:
        return self.nombre_oficial
