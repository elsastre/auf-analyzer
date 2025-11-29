param(
    [switch]$Install
)

$ErrorActionPreference = "Stop"

Write-Host "AUF Analyzer - script de desarrollo" -ForegroundColor Cyan

$backendPath  = Join-Path $PSScriptRoot "..\backend"
$frontendPath = Join-Path $PSScriptRoot "..\frontend"

if ($Install) {
    Write-Host "Paso 1/2: configurando backend (Python)..." -ForegroundColor Yellow
    Set-Location $backendPath

    if (-not (Test-Path ".venv")) {
        Write-Host "Creando entorno virtual .venv..." -ForegroundColor Yellow
        python -m venv .venv
    } else {
        Write-Host "Entorno virtual .venv ya existe, lo reutilizo." -ForegroundColor DarkGray
    }

    . .\.venv\Scripts\Activate.ps1

    Write-Host "Actualizando pip..." -ForegroundColor DarkGray
    python -m pip install -U pip

    if (Test-Path "requirements.txt") {
        Write-Host "Instalando dependencias de requirements.txt..." -ForegroundColor DarkGray
        pip install -r requirements.txt
    }

    Write-Host "Instalando FastAPI y Uvicorn..." -ForegroundColor DarkGray
    pip install fastapi "uvicorn[standard]"

    # 👇 AQUÍ el cambio importante: paréntesis explícitos
    if ((Test-Path "pyproject.toml") -or (Test-Path "setup.py")) {
        Write-Host "Instalando paquete local webscraper_futbol (pip install .)..." -ForegroundColor DarkGray
        pip install .
    }

    Write-Host "Paso 2/2: configurando frontend (Node)..." -ForegroundColor Yellow
    Set-Location $frontendPath

    if (-not (Test-Path "node_modules")) {
        Write-Host "Instalando dependencias de NPM..." -ForegroundColor DarkGray
        npm install
    } else {
        Write-Host "node_modules ya existe, salto npm install." -ForegroundColor DarkGray
    }

    Write-Host "Instalación inicial completada ✅" -ForegroundColor Green
}

Write-Host "Levantando backend (FastAPI) y frontend (Vite)..." -ForegroundColor Cyan

$backendCmd  = "Set-Location `"$backendPath`"; . .\.venv\Scripts\Activate.ps1; uvicorn api:app --reload --port 8000"
$frontendCmd = "Set-Location `"$frontendPath`"; npm run dev"

Start-Process powershell -ArgumentList "-NoExit", "-Command", $backendCmd
Start-Process powershell -ArgumentList "-NoExit", "-Command", $frontendCmd

Write-Host ""
Write-Host "Backend:  http://127.0.0.1:8000/docs" -ForegroundColor Green
Write-Host "Frontend: http://localhost:5173/" -ForegroundColor Green
