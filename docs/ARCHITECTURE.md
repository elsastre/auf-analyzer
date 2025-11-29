# AUF Analyzer — Architecture (high level)

AUF Analyzer is a portfolio-ready demo for exploring Uruguayan football data (AUF).

Stack:
- Frontend: React + Vite
- Backend: FastAPI
- Storage: local SQLite database generated from seed JSON (no live scraping required)
- Optional: background refresh endpoints (best-effort, may fail depending on external sources)

The default workflow is intentionally reproducible:
1) Docker Compose builds and runs frontend + API
2) On API startup, it creates/updates a local SQLite database from demo seeds
3) Endpoints serve data from that local dataset
