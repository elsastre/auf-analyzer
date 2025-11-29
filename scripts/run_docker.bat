@echo off
REM Levanta AUF Analyzer completo usando Docker Compose

docker info >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo No se detecto Docker. Instalalo o abre Docker Desktop antes de continuar.
    exit /b 1
)

echo Construyendo y levantando contenedores...
docker compose up --build
