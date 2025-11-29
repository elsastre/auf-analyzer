# AUF Analyzer — Analítica de fútbol uruguayo (Demo de portfolio)

AUF Analyzer es una demo reproducible para explorar datos del fútbol uruguayo (tablas, fixtures, goleadores e insights).
Está pensado para funcionar **sin depender de scraping en vivo**: por defecto genera un dataset local (SQLite) a partir de seeds.

## Stack
- FastAPI (Python)
- SQLite (generado desde seeds)
- React + Vite
- Docker Compose

## Ejecución rápida (Docker)
Requisitos: Docker Desktop

```bash
docker compose up -d --build
```

Abrir:
- Frontend: `http://localhost:5173`
- Docs API: `http://localhost:8000/docs`

## Demo
Ver `docs/DEMO.md`.

## Datos (importante)
- **Modo demo (recomendado):** seeds -> SQLite local al iniciar.
- **Refresh:** endpoints de actualización best-effort (no son necesarios para la demo).

## Endpoints útiles
- `GET /meta`
- `GET /tables`
- `GET /fixtures`
- `GET /scorers`
- `GET /stats/insights`
- `POST /ai/consultor`

## Reset seed
- `POST /admin/reseed` o borrar `backend/data/auf.db` y reiniciar.

