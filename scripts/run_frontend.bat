@echo off
echo [FRONTEND] Iniciando frontend...
cd /d "%~dp0auf-frontend"
echo [FRONTEND] Carpeta actual:
cd

if not exist node_modules (
  echo [FRONTEND] Ejecutando npm install...
  npm install
)

echo [FRONTEND] Levantando Vite (npm run dev)...
npm run dev
