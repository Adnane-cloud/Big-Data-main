"""
Tableau de bord Streamlit — Streaming musical (MongoDB + Spark)

Ce tableau de bord NE FAIT AUCUNE REQUÊTE sur la base : il lit uniquement les
fichiers JSON du dossier dashboard/data, exportés par mongoexport depuis les
collections benchmark_results (temps mesurés par le profiler MongoDB) et dash_*.

Lancement : il tourne dans le conteneur "dashboard" -> http://localhost:8501
"""
import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

DATA = Path(__file__).parent / "data"
st.set_page_config(page_title="Streaming musical — MongoDB", layout="wide")


@st.cache_data(ttl=15)
def charger(nom):
    fichier = DATA / f"{nom}.json"
    if not fichier.exists():
        return pd.DataFrame()
    return pd.DataFrame(json.loads(fichier.read_text(encoding="utf-8")))


bench = charger("benchmark_results")
catalogue = charger("catalogue_requetes")
if not bench.empty:
    # libellés (catalogue_requetes) ; temps retenu = max(durée serveur en ms, temps CPU) pour les opérations < 1 ms
    if "catalogue" in bench.columns:
        bench = bench.drop(columns=["catalogue"])
    for col in ["categorie", "description"]:
        if col not in bench.columns:
            bench[col] = None
        if not catalogue.empty:
            bench[col] = bench["_id"].map(catalogue.set_index("_id")[col]).fillna(bench[col])
    if "cpu_ms" in bench.columns:
        bench["ms"] = bench[["ms", "cpu_ms"]].max(axis=1).round(3)
    if "nb_operations" in bench.columns:
        bench["ms_par_operation"] = bench["ms"] / bench["nb_operations"].fillna(1)
    for col in ["nb_resultats", "docs_examines"]:
        if col not in bench.columns:
            bench[col] = None
st.title("Streaming musical — tableau de bord")
st.caption("Source : exports mongoexport des collections benchmark_results (temps mesurés par le profiler MongoDB) "
           "et dash_*. Aucune requête n'est exécutée par ce tableau de bord. Données rechargées toutes les 15 s.")

if bench.empty:
    st.error("Aucun fichier trouvé dans dashboard/data. Lancez d'abord run_all.ps1 (ou lancer_projet.bat).")
    st.stop()

onglets = st.tabs(["Performances", "Natif vs Spark", "Cohérence & index", "Écoutes", "Résultats Spark"])

# --------------------------------------------------------------------------- 1
with onglets[0]:
    st.subheader("Temps d'exécution des 30 requêtes")
    ids = [f"T{i:02d}" for i in range(1, 16)] + [f"A{i}" for i in range(16, 26)] + [f"S{i}" for i in range(26, 31)]
    w = bench[bench["_id"].isin(ids)].copy()
    w["ordre"] = w["_id"].apply(ids.index)
    w = w.sort_values("ordre")
    c1, c2, c3 = st.columns(3)
    for col, cat, lib in [(c1, "transactionnelle", "Transactionnelles (T)"),
                          (c2, "analytique", "Analytiques natives (A)"), (c3, "spark", "Spark (S)")]:
        sous = w[w["categorie"] == cat]
        col.metric(lib, f"{len(sous)} requêtes", f"médiane {sous['ms'].median():.1f} ms" if len(sous) else "")
    fig = px.bar(w, x="_id", y="ms", color="categorie", log_y=True, hover_data=["description", "nb_resultats"],
                 labels={"_id": "Requête", "ms": "Temps (ms, échelle log)", "categorie": "Catégorie"})
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(w[["_id", "categorie", "description", "ms", "nb_resultats"]], hide_index=True, use_container_width=True)

# --------------------------------------------------------------------------- 2
with onglets[1]:
    st.subheader("Même requête : agrégation MongoDB native vs Spark (lecture via le connecteur)")
    lignes = []
    for q in ["A16", "A19", "A21"]:
        n = bench.loc[bench["_id"] == q, "ms"]
        s = bench.loc[bench["_id"] == f"SPARK_{q}", "ms"]
        if len(n) and len(s):
            lignes += [{"requete": q, "moteur": "MongoDB natif", "ms": float(n.iloc[0])},
                       {"requete": q, "moteur": "Spark", "ms": float(s.iloc[0])}]
    if lignes:
        df = pd.DataFrame(lignes)
        st.plotly_chart(px.bar(df, x="requete", y="ms", color="moteur", barmode="group", text_auto=".0f",
                               labels={"ms": "Temps (ms)", "requete": "Requête"}), use_container_width=True)
        st.dataframe(df.pivot(index="requete", columns="moteur", values="ms"), use_container_width=True)
    else:
        st.info("Lancez le job Spark pour obtenir la comparaison.")

# --------------------------------------------------------------------------- 3
with onglets[2]:
    st.subheader("Write concern et read concern")
    coh = bench[bench["categorie"] == "coherence"].copy()
    ecr = coh[coh["_id"].str.match(r"^W(1|MAJ)_")].copy()
    if not ecr.empty:
        ecr["ms_par_ecriture"] = ecr["ms_par_operation"]
        ecr["noeuds"] = ecr["_id"].str.extract(r"_(\dn)$")[0].str.replace("n", " nœuds")
        ecr["mode"] = ecr["_id"].str.extract(r"^(W1|WMAJ)")[0].map({"W1": "w:1", "WMAJ": "w:majority"})
        ecr = ecr.sort_values(["noeuds", "mode"], ascending=[False, True])
        st.plotly_chart(px.bar(ecr, x="mode", y="ms_par_ecriture", color="noeuds", barmode="group", text_auto=".2f",
                               labels={"ms_par_ecriture": "ms par écriture", "mode": ""}), use_container_width=True)
    st.dataframe(coh[["_id", "description", "ms"]], hide_index=True, use_container_width=True)

    st.subheader("Avec index vs sans index (COLLSCAN)")
    idx = bench[bench["_id"].str.startswith("IDX_")].copy()
    if not idx.empty:
        idx["requete"] = idx["_id"].str.extract(r"IDX_(\w+?)_")[0]
        idx["mode"] = idx["_id"].str.endswith("_avec").map({True: "avec index", False: "sans index"})
        st.plotly_chart(px.bar(idx, x="requete", y="ms", color="mode", barmode="group", text_auto=".1f",
                               hover_data=["docs_examines"], labels={"ms": "Temps (ms)"}), use_container_width=True)
        st.dataframe(idx[["requete", "mode", "ms", "docs_examines"]], hide_index=True, use_container_width=True)

# --------------------------------------------------------------------------- 4
with onglets[3]:
    c1, c2 = st.columns(2)
    jour = charger("dash_ecoutes_par_jour")
    if not jour.empty:
        c1.plotly_chart(px.line(jour, x="_id", y="ecoutes", markers=True, title="A16 — Écoutes par jour (30 j)", labels={"_id": "jour"}),
                        use_container_width=True)
    heure = charger("dash_ecoutes_par_heure")
    if not heure.empty:
        c2.plotly_chart(px.bar(heure, x="_id", y="ecoutes", title="A22 — Écoutes par heure", labels={"_id": "heure"}), use_container_width=True)
    saut = charger("dash_saut_par_genre")
    if not saut.empty:
        c1.plotly_chart(px.bar(saut.sort_values("taux_saut_pct"), x="taux_saut_pct", y="_id", orientation="h", labels={"_id": "genre"},
                               title="A19 — Taux de titres sautés par genre (%)"), use_container_width=True)
    app = charger("dash_duree_par_appareil")
    if not app.empty:
        c2.plotly_chart(px.bar(app, x="_id", y="duree_moyenne_s", title="A20 — Durée moyenne par appareil (s)", labels={"_id": "appareil"}),
                        use_container_width=True)
    abo = charger("dash_users_abonnement_pays")
    if not abo.empty:
        abo["pays"] = abo["_id"].apply(lambda d: d["pays"])
        abo["abonnement"] = abo["_id"].apply(lambda d: d["abonnement"])
        st.plotly_chart(px.bar(abo, x="pays", y="utilisateurs", color="abonnement",
                               title="A21 — Utilisateurs par abonnement et par pays"), use_container_width=True)
    top = charger("dash_top_titres_semaine")
    if not top.empty:
        top["artiste"] = top["titre"].apply(lambda l: l[0].get("artiste_nom", "") if l else "")
        top["titre"] = top["titre"].apply(lambda l: l[0].get("titre", "") if l else "")
        st.markdown("**A17 — Top 10 des titres de la semaine**")
        st.dataframe(top.rename(columns={"_id": "titre_id"})[["titre_id", "titre", "artiste", "ecoutes"]], hide_index=True, use_container_width=True)
    ma = charger("dash_top_artistes_ma")
    if not ma.empty:
        ma["artiste"] = ma["artiste"].apply(lambda l: l[0].get("nom", "") if l else "")
        st.plotly_chart(px.bar(ma, x="artiste", y="ecoutes", title="A18 — Top 10 des artistes au Maroc"), use_container_width=True)

# --------------------------------------------------------------------------- 5
with onglets[4]:
    s29 = charger("dash_spark_abonnements")
    if not s29.empty:
        st.plotly_chart(px.bar(s29, x="abonnement", y=["ecoutes_par_utilisateur", "taux_saut_pct"], barmode="group",
                               title="S29 — Activité par type d'abonnement"), use_container_width=True)
    s28 = charger("dash_spark_sessions")
    if not s28.empty:
        r = s28.iloc[0]
        c1, c2, c3 = st.columns(3)
        c1.metric("S28 — Sessions", f"{int(r['nb_sessions']):,}".replace(",", " "))
        c2.metric("Durée moyenne", f"{r['duree_moyenne_min']} min")
        c3.metric("Titres par session", r["titres_par_session"])
    for nom, lib in [("dash_spark_tendances", "S26 — Tendances (progression semaine/semaine)"),
                     ("dash_spark_recommandations", "S27 — Recommandations ALS"),
                     ("dash_spark_artistes_similaires", "S30 — Artistes similaires")]:
        df = charger(nom)
        if not df.empty:
            st.markdown(f"**{lib}**")
            st.dataframe(df, hide_index=True, use_container_width=True)
