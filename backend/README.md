# Web Scraper Fútbol Uruguay 🇺🇾⚽

Scraper de datos reales de la Primera División Uruguaya usando worldfootball.net como fuente principal (sin Selenium dentro de Docker).

## 🚀 ¿Qué hace?

- Extrae la tabla de posiciones del fútbol uruguayo (Liga Uruguaya)
- Parseo completo de equipos + estadísticas
- Guarda resultados en CSV dentro de `data/`
- Funciona online sin Selenium dentro de Docker (aiohttp + BeautifulSoup)

✅ Código ejecutable desde terminal
✅ Datos reales
✅ Entrega lista para presentación

---

## 🧪 Ejemplo de uso

```bash
python -m webscraper_futbol
```

## 🧰 Modo sin datos y CSV de ejemplo

- El backend ahora resuelve el CSV de standings usando un helper con prioridad: ruta explícita, CSV generado (`data/standings_uruguay.csv`), CSV de ejemplo (`data/standings_uruguay_sample.csv`) y, si no hay nada, responde con estructuras vacías.
- Los endpoints devuelven 200 y listas vacías cuando no hay datos locales, permitiendo que el frontend siga funcionando. Solo `/torneo/equipos/buscar` mantiene el 404 si no existe el dataset o el equipo buscado.
- `backend/data/standings_uruguay_sample.csv` contiene un pequeño set de prueba para levantar la app en modo demo cuando el scraping falle.
- `/standings/refresh` usa worldfootball y, si falla, intenta mantener los datos previos o de ejemplo devolviendo un `warning` en lugar de romper con 500.
