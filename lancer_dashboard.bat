@echo off
REM Demarre uniquement le tableau de bord Streamlit (dans Docker) et ouvre le navigateur.
cd /d "%~dp0"
docker compose -f docker/docker-compose.yml up -d --build dashboard
timeout /t 5 >nul
start http://localhost:8501
