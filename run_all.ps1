# =============================================================================
# run_all.ps1 - Execute tout le workflow (Windows PowerShell), depuis la racine du projet :
#   powershell -ExecutionPolicy Bypass -File .\run_all.ps1
# Les requetes sont des fichiers MongoDB (.mongodb) envoyes tels quels au shell MongoDB.
# (fichier volontairement sans accents : Windows PowerShell 5.1 lit les .ps1 en ANSI)
# =============================================================================
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$URI = "mongodb://mongo1:27017,mongo2:27018,mongo3:27019/music_streaming?replicaSet=rs0"

# Execute un fichier .mongodb et affiche les resultats (sans les invites du shell)
function Requete($fichier) {
    Write-Host ""
    Write-Host ">>> $fichier" -ForegroundColor Cyan
    docker exec mongo1 sh -c "mongosh --quiet '$URI' < /scripts/$fichier" 2>&1 |
        ForEach-Object { ($_ -replace '^rs0 \[\w+\] music_streaming> (\.\.\. )*', '') } |
        Where-Object { $_.Trim() -ne '' -and $_ -notmatch "^\s*'\d+': ObjectId\(" }
}

function AttendrePrimaire($attendu) {
    Write-Host "Attente du primaire $attendu ..."
    for ($i = 0; $i -lt 60; $i++) {
        $p = docker exec mongo2 mongosh --quiet $URI --eval "db.hello().primary" 2>$null
        if ($p -and $p.Trim() -like "$attendu*") { Write-Host "Primaire : $p"; return }
        Start-Sleep -Seconds 3
    }
    Write-Host "Primaire $attendu non detecte, on continue." -ForegroundColor Yellow
}

# --- 0. Docker est-il demarre ? ---
docker info *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Docker Desktop n'est pas demarre. Lancez-le puis relancez ce script." -ForegroundColor Red
    exit 1
}

Write-Host ">>> 1. Demarrage de l'infrastructure (MongoDB, Spark, Streamlit)" -ForegroundColor Green
cmd /c "docker compose -f docker/docker-compose.yml up -d --build 2>&1"
AttendrePrimaire "mongo1"

Write-Host ""
Write-Host ">>> 2. Initialisation : chargement des donnees, index, profiler" -ForegroundColor Green
Requete "01_initialisation/01_chargement_donnees.mongodb"
Requete "01_initialisation/02_index.mongodb"
Requete "01_initialisation/03_catalogue_et_profiler.mongodb"

Write-Host ""
Write-Host ">>> 3. Requetes transactionnelles T01 a T15 puis analytiques A16 a A25" -ForegroundColor Green
Get-ChildItem mongo/02_requetes/T*.mongodb | Sort-Object Name | ForEach-Object { Requete "02_requetes/$($_.Name)" }
Get-ChildItem mongo/02_requetes/A*.mongodb | Sort-Object Name | ForEach-Object { Requete "02_requetes/$($_.Name)" }

Write-Host ""
Write-Host ">>> 4. Comparaisons : index, modelisation, coherence" -ForegroundColor Green
Requete "03_comparaisons/C01_effet_des_index.mongodb"
Requete "03_comparaisons/C02_imbrication_vs_lookup.mongodb"
Requete "03_comparaisons/C03_coherence_3_noeuds.mongodb"

Write-Host ""
Write-Host ">>> 5. Test avec un noeud arrete (mongo3)" -ForegroundColor Green
docker stop mongo3
Start-Sleep -Seconds 20
Requete "03_comparaisons/C04_coherence_2_noeuds.mongodb"
docker start mongo3
Start-Sleep -Seconds 15

Write-Host ""
Write-Host ">>> 6. Temps mesures par le profiler + collections du tableau de bord" -ForegroundColor Green
Requete "04_resultats/R01_temps_des_requetes.mongodb"
Requete "04_resultats/R02_tableau_de_bord.mongodb"

Write-Host ""
Write-Host ">>> 7. Spark S26-S30 (connecteur MongoDB, cluster 1 master + 2 workers)" -ForegroundColor Green
cmd /c "docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 --conf spark.jars.ivy=/tmp/.ivy2 /opt/spark/apps/spark_queries.py 2>&1"

Write-Host ""
Write-Host ">>> 8. Export des resultats pour le tableau de bord (mongoexport)" -ForegroundColor Green
$collections = @("benchmark_results", "catalogue_requetes", "dash_ecoutes_par_jour", "dash_top_titres_semaine", "dash_top_artistes_ma",
    "dash_saut_par_genre", "dash_duree_par_appareil", "dash_users_abonnement_pays", "dash_ecoutes_par_heure",
    "dash_albums_2020", "dash_spark_tendances", "dash_spark_recommandations", "dash_spark_sessions",
    "dash_spark_abonnements", "dash_spark_artistes_similaires")
foreach ($c in $collections) {
    cmd /c "docker exec mongo1 mongoexport --quiet --uri=$URI --collection=$c --jsonArray --out=/export/$c.json 2>&1"
    Write-Host "  export $c.json"
}

Write-Host ""
Write-Host ">>> 9. Bascule du primaire" -ForegroundColor Green
Requete "03_comparaisons/C05_bascule_primaire.mongodb"
Start-Sleep -Seconds 15
Requete "03_comparaisons/C06_apres_bascule.mongodb"

Write-Host ""
Write-Host ">>> Termine. Tableau de bord : http://localhost:8501   -   Spark : http://localhost:8085" -ForegroundColor Green
Start-Process "http://localhost:8501"
