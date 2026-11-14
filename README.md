# Sujet 06 — Streaming musical (MongoDB + Spark)

Les requêtes sont écrites en **MongoDB pur (MQL)**, un fichier `.mongodb` par requête, avec les
commandes et opérateurs du cours : `insertOne/insertMany`, `find/findOne/countDocuments`, projection,
`sort/limit`, `$set/$inc/$push/$addToSet/$pull`, `deleteMany`, et les pipelines
`$match/$group/$sort/$limit/$project/$addFields/$lookup`.
Les données sont chargées comme dans le TP (`load("shop_maroc.js")`) par le script
`donnees_streaming.js` (insertMany).

Les temps sont mesurés **par MongoDB lui-même**, grâce au profiler (`system.profile`). Chaque requête
porte un commentaire (`comment: "T01"`), ce qui permet de retrouver sa durée.

```
docker/        docker-compose.yml (MongoDB x3, Spark 1+2, Streamlit), Dockerfiles
mongo/
  01_initialisation/   01_chargement_donnees (+ donnees_streaming.js), 02_index, 03_catalogue_et_profiler
  02_requetes/         T01 à T15, A16 à A25 (un fichier par requête)
  03_comparaisons/     C01 index, C02 imbrication vs $lookup, C03/C04 cohérence (3 et 2 nœuds), C05/C06 bascule
  04_resultats/        R01 temps mesurés par le profiler, R02 collections du tableau de bord
spark/         spark_queries.py (S26 à S30, connecteur MongoDB)
dashboard/     app.py (Streamlit, dans Docker) + data/ (exports mongoexport)
run_all.ps1    tout le workflow ; lancer_projet.bat pour le lancer par double-clic
```

## Lancer

- **Tout le projet** : double-cliquer sur `lancer_projet.bat` (Docker Desktop doit être démarré).
  Le tableau de bord s'ouvre ensuite sur http://localhost:8501.
- **Uniquement le tableau de bord** : double-cliquer sur `lancer_dashboard.bat`.
- **Une seule requête** (PowerShell, depuis la racine du projet) :

```powershell
docker exec mongo1 sh -c "mongosh --quiet 'mongodb://mongo1:27017,mongo2:27018,mongo3:27019/music_streaming?replicaSet=rs0' < /scripts/02_requetes/T04_50_dernieres_ecoutes.mongodb"
```

Les requêtes peuvent aussi être copiées-collées telles quelles dans MongoDB Compass (onglet
« MONGOSH ») ou dans un playground de l'extension MongoDB pour VS Code.

Interfaces : tableau de bord http://localhost:8501 — Spark http://localhost:8085
