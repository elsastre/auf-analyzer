# AUF Analyzer — Uruguayan Football Analytics (Portfolio Demo)

AUF Analyzer is a reproducible demo to explore Uruguayan football data (tables, fixtures, scorers and basic insights).
It is designed to run **without relying on fragile live scraping**: the backend seeds a local SQLite dataset by default.

## Tech stack
- FastAPI (Python)
- SQLite (generated from seed JSON)
- React + Vite
- Docker Compose
- Optional: a lightweight local "AI advisor" endpoint for matchups

## Quickstart (Docker)
Prereqs: Docker Desktop

```bash
docker compose up -d --build
```

Open:
- Frontend: `http://localhost:5173`
- API docs: `http://localhost:8000/docs`

## Demo (60s)
See `docs/DEMO.md`.

## Data approach (important)
- **Default mode (recommended):** local demo dataset (seed JSON -> SQLite on startup).
- **Refresh endpoints:** some endpoints can attempt to refresh standings/cards from external sources,
  but they are best-effort and not required for the demo.

## Useful endpoints
- `GET /meta`
- `GET /tables`
- `GET /fixtures`
- `GET /scorers`
- `GET /stats/insights`
- `POST /ai/consultor`

## Reset demo data
To reseed the database:

- API endpoint: `POST /admin/reseed`
- Or delete `backend/data/auf.db` and restart the backend/container.

## Repository layout
- `backend/` FastAPI app + local data seeds
- `frontend/` React UI
- `docs/` demo + architecture notes
- `scripts/` helper scripts (Windows)

Spanish README: `README.es.md`
