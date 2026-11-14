@echo off
REM Double-cliquez sur ce fichier pour lancer tout le projet (Docker Desktop doit etre demarre).
REM La sortie s'affiche dans la fenetre ET est enregistree dans resultats_execution.log
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -Command "& '.\run_all.ps1' *>&1 | Tee-Object -FilePath 'resultats_execution.log'"
echo.
echo Termine. Le tableau de bord est sur http://localhost:8501
pause
