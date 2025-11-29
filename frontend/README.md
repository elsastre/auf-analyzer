# AUF Analyzer — Frontend (React + Vite)

This frontend consumes the FastAPI backend.

## Run with Docker (recommended)
From repo root:

```bash
docker compose up -d --build
```

Open: http://localhost:5173

## Development (without Docker)
- Configure API base in `.env` (or defaults to `http://localhost:8000`)
- Install & run:

```bash
npm install
npm run dev
```
