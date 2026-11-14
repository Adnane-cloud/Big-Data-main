"""
spark_queries.py — Requêtes distribuées S26 à S30 + comparaison natif vs Spark (A16, A19, A21)

Les données sont lues DIRECTEMENT dans MongoDB par le connecteur officiel
MongoDB Spark Connector (format "mongodb") : aucune requête pymongo, aucun pandas.
Les résultats et les temps sont réécrits dans MongoDB (collections benchmark_results
et dash_spark_*) par le même connecteur.

Lancement (depuis la machine hôte) :
  docker exec -it spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
      --conf spark.jars.ivy=/tmp/.ivy2 \
      /opt/spark/apps/spark_queries.py
"""
import time
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS

URI = "mongodb://mongo1:27017,mongo2:27018,mongo3:27019/music_streaming?replicaSet=rs0"
DB = "music_streaming"

spark = (SparkSession.builder
         .appName("Streaming-Musical-S26-S30")
         .config("spark.mongodb.read.connection.uri", URI)
         .config("spark.mongodb.write.connection.uri", URI)
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.driver.host", "spark-master")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

resultats = []            # temps mesurés, renvoyés dans MongoDB à la fin


def lire(collection):
    """Lecture d'une collection MongoDB via le connecteur (DataFrame distribué)."""
    return spark.read.format("mongodb").option("database", DB).option("collection", collection).load()


def ecrire(df, collection):
    """Écriture d'un DataFrame dans MongoDB (remplace la collection)."""
    df.write.format("mongodb").mode("overwrite").option("database", DB).option("collection", collection).save()


def chrono(id_, description, action, categorie="spark"):
    t0 = time.perf_counter()
    res = action()
    ms = (time.perf_counter() - t0) * 1000
    nb = len(res) if isinstance(res, list) else 1
    print(f"[{id_:<12}] {description:<58} {ms:>10.1f} ms   ({nb} résultat(s))")
    resultats.append(Row(_id=id_, categorie=categorie, description=description, ms=round(ms, 3),
                         repetitions=1, nb_resultats=nb, execute_le=datetime.now().isoformat()))
    return res


print("=" * 95)
print("  SPARK — chargement des collections via le connecteur MongoDB")
print("=" * 95)

# Chargement + mise en cache (mesuré séparément)
listens = lire("listens").select("utilisateur_id", "titre_id", "artiste_id", "genre",
                                 "horodatage", "duree_ecoutee", "sautee")
users = lire("users").select(F.col("_id").alias("user_id"), "abonnement", "pays")
tracks = lire("tracks").select(F.col("_id").alias("track_id"), "titre", "artiste_nom")
artists = lire("artists").select(F.col("_id").alias("art_id"), F.col("nom").alias("artiste_nom"))

chrono("SPARK_LOAD", "Chargement + cache de listens (connecteur MongoDB)",
       lambda: [listens.cache().count()])
users.cache().count(); tracks.cache().count(); artists.cache().count()

print("\n" + "=" * 95)
print("  REQUÊTES DISTRIBUÉES (S26 à S30)")
print("=" * 95)

# -----------------------------------------------------------------------------
# S26 — Tendances : progression des écoutes d'une semaine à l'autre (fonction lag)
# -----------------------------------------------------------------------------
def s26():
    hebdo = (listens.withColumn("semaine", F.date_trunc("week", "horodatage"))
             .groupBy("titre_id", "semaine").agg(F.count("*").alias("ecoutes")))
    w = Window.partitionBy("titre_id").orderBy("semaine")
    derniere = hebdo.agg(F.max("semaine")).first()[0]
    tendances = (hebdo.withColumn("ecoutes_sem_prec", F.lag("ecoutes", 1, 0).over(w))
                 .filter(F.col("semaine") == derniere)
                 .withColumn("progression", F.col("ecoutes") - F.col("ecoutes_sem_prec"))
                 .join(tracks, F.col("titre_id") == F.col("track_id"))
                 .select("titre_id", "titre", "artiste_nom", "ecoutes_sem_prec", "ecoutes", "progression")
                 .orderBy(F.desc("progression")).limit(10))
    ecrire(tendances, "dash_spark_tendances")
    return tendances.collect()

r = chrono("S26", "Tendances semaine/semaine (lag)", s26)
for x in r[:3]:
    print(f"      {x.titre_id} {x.titre:<20} {x.ecoutes_sem_prec} -> {x.ecoutes} (+{x.progression})")

# -----------------------------------------------------------------------------
# S27 — Recommandation ALS (MLlib, feedback implicite = nombre d'écoutes) : top 10
# -----------------------------------------------------------------------------
UTILISATEUR = "usr_000001"

def s27():
    notes = listens.groupBy("utilisateur_id", "titre_id").agg(F.count("*").cast("float").alias("note"))
    idx_u = StringIndexer(inputCol="utilisateur_id", outputCol="u").fit(notes)
    idx_t = StringIndexer(inputCol="titre_id", outputCol="t").fit(notes)
    donnees = idx_t.transform(idx_u.transform(notes))
    modele = ALS(userCol="u", itemCol="t", ratingCol="note", implicitPrefs=True,
                 rank=10, maxIter=5, regParam=0.1, coldStartStrategy="drop", seed=42).fit(donnees)
    cible = donnees.filter(F.col("utilisateur_id") == UTILISATEUR).select("u").distinct()
    recos = (modele.recommendForUserSubset(cible, 10)
             .select(F.explode("recommendations").alias("r"))
             .select(F.col("r.t").alias("t"), F.col("r.rating").alias("score")))
    libelles = spark.createDataFrame([(float(i), v) for i, v in enumerate(idx_t.labels)], ["t", "titre_id"])
    top = (recos.join(libelles, "t").join(tracks, F.col("titre_id") == F.col("track_id"))
           .select(F.lit(UTILISATEUR).alias("utilisateur_id"), "titre_id", "titre", "artiste_nom",
                   F.round("score", 4).alias("score"))
           .orderBy(F.desc("score")))
    ecrire(top, "dash_spark_recommandations")
    return top.collect()

r = chrono("S27", f"Recommandation ALS top 10 pour {UTILISATEUR}", s27)
for x in r[:3]:
    print(f"      {x.titre_id} {x.titre:<20} score={x.score}")

# -----------------------------------------------------------------------------
# S28 — Sessions d'écoute (nouvelle session si pause > 30 min) et durée moyenne
# -----------------------------------------------------------------------------
def s28():
    w = Window.partitionBy("utilisateur_id").orderBy("horodatage")
    d = (listens.withColumn("prec", F.lag("horodatage").over(w))
         .withColumn("pause_s", F.unix_timestamp("horodatage") - F.unix_timestamp("prec"))
         .withColumn("nouvelle", F.when(F.col("pause_s").isNull() | (F.col("pause_s") > 1800), 1).otherwise(0))
         .withColumn("session", F.sum("nouvelle").over(w)))
    sessions = d.groupBy("utilisateur_id", "session").agg(
        F.count("*").alias("nb_titres"),
        F.sum("duree_ecoutee").alias("duree_s"))
    stats = sessions.agg(F.count("*").alias("nb_sessions"),
                         F.round(F.avg("duree_s") / 60, 2).alias("duree_moyenne_min"),
                         F.round(F.avg("nb_titres"), 2).alias("titres_par_session"))
    ecrire(stats, "dash_spark_sessions")
    return stats.collect()

r = chrono("S28", "Sessions (pause > 30 min) et durée moyenne", s28)
print(f"      {r[0].nb_sessions} sessions, durée moyenne {r[0].duree_moyenne_min} min, "
      f"{r[0].titres_par_session} titres/session")

# -----------------------------------------------------------------------------
# S29 — Activité comparée gratuit / premium / famille (jointure users x listens)
# -----------------------------------------------------------------------------
def s29():
    act = (listens.join(users, listens.utilisateur_id == users.user_id)
           .groupBy("abonnement").agg(
               F.count("*").alias("ecoutes"),
               F.countDistinct("utilisateur_id").alias("utilisateurs_actifs"),
               F.round(F.avg("duree_ecoutee"), 1).alias("duree_moyenne_s"),
               F.round(F.avg(F.col("sautee").cast("int")) * 100, 2).alias("taux_saut_pct"))
           .withColumn("ecoutes_par_utilisateur", F.round(F.col("ecoutes") / F.col("utilisateurs_actifs"), 2))
           .orderBy(F.desc("ecoutes")))
    ecrire(act, "dash_spark_abonnements")
    return act.collect()

r = chrono("S29", "Activité gratuit vs premium (jointure)", s29)
for x in r:
    print(f"      {x.abonnement:<8} {x.ecoutes} écoutes, {x.ecoutes_par_utilisateur} écoutes/utilisateur, saut {x.taux_saut_pct} %")

# -----------------------------------------------------------------------------
# S30 — Artistes similaires : nombre d'auditeurs communs par paire d'artistes
# -----------------------------------------------------------------------------
def s30():
    ua = listens.select("utilisateur_id", "artiste_id").distinct()
    paires = (ua.alias("a").join(ua.alias("b"),
              (F.col("a.utilisateur_id") == F.col("b.utilisateur_id")) &
              (F.col("a.artiste_id") < F.col("b.artiste_id")))
              .groupBy(F.col("a.artiste_id").alias("artiste_1"), F.col("b.artiste_id").alias("artiste_2"))
              .agg(F.count("*").alias("auditeurs_communs"))
              .orderBy(F.desc("auditeurs_communs")).limit(10))
    a1, a2 = artists.alias("x"), artists.alias("y")
    top = (paires.join(a1, F.col("artiste_1") == F.col("x.art_id"))
           .join(a2, F.col("artiste_2") == F.col("y.art_id"))
           .select("artiste_1", F.col("x.artiste_nom").alias("nom_1"),
                   "artiste_2", F.col("y.artiste_nom").alias("nom_2"), "auditeurs_communs")
           .orderBy(F.desc("auditeurs_communs")))
    ecrire(top, "dash_spark_artistes_similaires")
    return top.collect()

r = chrono("S30", "Artistes similaires (auditeurs communs)", s30)
for x in r[:3]:
    print(f"      {x.nom_1} <-> {x.nom_2} : {x.auditeurs_communs} auditeurs communs")

# =============================================================================
# COMPARAISON NATIF vs SPARK — mêmes requêtes que A16, A19, A21 (lecture à froid
# depuis MongoDB + calcul ; le temps natif correspondant est mesuré par 04_analytical.js)
# =============================================================================
print("\n" + "=" * 95)
print("  COMPARAISON NATIF vs SPARK (A16, A19, A21) — lecture MongoDB + calcul")
print("=" * 95)

def spark_a16():
    limite = datetime(2026, 10, 16)   # même période que A16 (données datées jusqu'au 15/11/2026)
    return (lire("listens").filter(F.col("horodatage") >= F.lit(limite))
            .groupBy(F.date_format("horodatage", "yyyy-MM-dd").alias("jour"))
            .agg(F.count("*").alias("ecoutes")).orderBy("jour").collect())

def spark_a19():
    return (lire("listens").groupBy("genre")
            .agg(F.count("*").alias("total"), F.sum(F.col("sautee").cast("int")).alias("sautes"))
            .withColumn("taux_saut_pct", F.round(F.col("sautes") / F.col("total") * 100, 2))
            .orderBy(F.desc("taux_saut_pct")).collect())

def spark_a21():
    return lire("users").groupBy("pays", "abonnement").agg(F.count("*").alias("utilisateurs")).collect()

chrono("SPARK_A16", "A16 Écoutes par jour sur 30 jours (Spark)", spark_a16, "natif_vs_spark")
chrono("SPARK_A19", "A19 Taux de saut par genre (Spark)", spark_a19, "natif_vs_spark")
chrono("SPARK_A21", "A21 Utilisateurs par abonnement et pays (Spark)", spark_a21, "natif_vs_spark")

# Enregistrement des temps Spark dans benchmark_results (upsert sur _id par le connecteur)
(spark.createDataFrame(resultats).write.format("mongodb").mode("append")
 .option("database", DB).option("collection", "benchmark_results").save())
print("\n[OK] Temps Spark enregistrés dans music_streaming.benchmark_results")
spark.stop()
