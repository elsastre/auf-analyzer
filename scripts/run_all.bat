@echo off
setlocal
echo [RUN_ALL] Abriendo backend y frontend en dos consolas...
start "AUF Backend" "%~dp0run_backend.bat"
start "AUF Frontend" "%~dp0run_frontend.bat"
endlocal
