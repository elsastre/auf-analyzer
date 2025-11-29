@echo off
echo [BACKEND] Iniciando backend...
cd /d "%~dp0backend"
echo [BACKEND] Carpeta actual:
cd

if not exist .venv (
  echo [BACKEND] Creando entorno virtual .venv...
  python -m venv .venv
)

call .venv\Scripts\activate.bat

echo [BACKEND] Instalando dependencias (requirements + fastapi + uvicorn)...
pip install -r requirements.txt fastapi uvicorn

echo [BACKEND] Levantando API con Uvicorn en puerto 8000...
python -m uvicorn api:app --reload --port 8000
