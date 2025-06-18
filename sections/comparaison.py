import streamlit as st
import time
import pandas as pd
import os

def render(df_csv):
    st.title("Comparaison des Formats de Fichier")

    parquet_path = "openfood_clean.parquet"
    json_path = "openfood_clean.json"

    if not os.path.exists(parquet_path) or not os.path.exists(json_path):
        st.warning("Fichiers Parquet/JSON absents. Génération en cours...")

        # Convertir en Pandas pour sauvegarde manuelle
        sample_df = df_csv.toPandas()

        # Corriger les colonnes datetime pour compatibilité Spark
        for col in sample_df.select_dtypes(include=["datetime64[ns]"]).columns:
            sample_df[col] = sample_df[col].astype(str)

        sample_df.to_parquet(parquet_path, index=False)
        sample_df.to_json(json_path, orient="records", lines=True)

        st.success("Fichiers générés avec succès.")

    st.subheader("Temps de chargement via Spark")

    spark = df_csv.sparkSession
    times = {}

    start = time.time()
    df_csv_loaded = spark.read.option("header", True).option("inferSchema", True).option("sep", "\t").csv("openfood.csv")
    times["CSV"] = round(time.time() - start, 4)

    start = time.time()
    df_parquet = spark.read.parquet(parquet_path)
    times["Parquet"] = round(time.time() - start, 4)

    start = time.time()
    df_json = spark.read.json(json_path)
    times["JSON"] = round(time.time() - start, 4)

    st.table([
        {"Format": k, "Temps de chargement (s)": v}
        for k, v in times.items()
    ])

    st.success("Comparaison terminée. Tous les fichiers ont été lus avec Spark.")
