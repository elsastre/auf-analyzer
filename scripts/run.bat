@echo off
setlocal

REM ==== ENTRADA PRINCIPAL (sin par?metros) ====
if "%1"=="backend" goto backend
if "%1"=="frontend" goto frontend

echo [RUN] Abriendo backend y frontend en dos consolas...
start "AUF Backend" "%~f0" backend
start "AUF Frontend" "%~f0" frontend
goto :eof

REM ==== BACKEND ====
:backend
echo [BACKEND] Iniciando backend...
cd /d "%~dp0backend"
echo [BACKEND] Carpeta actual:
cd

if not exist .venv (
  echo [BACKEND] Creando entorno virtual .venv y configurando dependencias...
  python -m venv .venv
  call .venv\Scripts\activate.bat
  pip install -r requirements.txt fastapi "uvicorn[standard]"
  pip install -e .
) else (
  echo [BACKEND] Usando entorno virtual existente...
  call .venv\Scripts\activate.bat
)

echo [BACKEND] Levantando API con Uvicorn en puerto 8000...
python -m uvicorn api:app --reload --port 8000
goto :eof

REM ==== FRONTEND ====
:frontend
echo [FRONTEND] Iniciando frontend...
cd /d "%~dp0auf-frontend"
echo [FRONTEND] Carpeta actual:
cd

if not exist node_modules (
  echo [FRONTEND] Ejecutando npm install por primera vez...
  npm install
)

echo [FRONTEND] Levantando Vite (npm run dev)...
npm run dev
goto :eof
