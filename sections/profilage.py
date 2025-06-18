import streamlit as st
from pyspark.sql.functions import col, count, when, isnan
from pyspark.sql.types import NumericType

def render(_):
    st.title("Profilage des Données – Format Parquet")

    parquet_path = "openfood_clean.parquet"
    spark = _.sparkSession

    st.info(f"Chargement du fichier : `{parquet_path}`")
    df = spark.read.parquet(parquet_path)

    st.subheader("Aperçu général")
    nb_lignes = df.count()
    nb_colonnes = len(df.columns)
    st.write(f"**Nombre de lignes :** {nb_lignes}")
    st.write(f"**Nombre de colonnes :** {nb_colonnes}")

    with st.expander("Voir le schéma Spark"):
        schema_str = df._jdf.schema().treeString()
        st.code(schema_str, language="text")

    st.subheader("Valeurs nulles (%) par colonne")
    null_exprs = []
    for c in df.columns:
        field = df.schema[c]
        if isinstance(field.dataType, NumericType):
            expr = (count(when(col(c).isNull() | isnan(col(c)), c)) / nb_lignes * 100).alias(c)
        else:
            expr = (count(when(col(c).isNull(), c)) / nb_lignes * 100).alias(c)
        null_exprs.append(expr)

    nulls_df = df.select(null_exprs).toPandas().T.reset_index()
    nulls_df.columns = ["Colonne", "Pourcentage de valeurs nulles"]
    st.dataframe(nulls_df)

    st.subheader("Nombre de doublons")
    nb_doublons = df.count() - df.dropDuplicates().count()
    st.write(f"**Doublons détectés :** {nb_doublons}")

    st.subheader("Statistiques descriptives (variables numériques)")
    numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]
    if numeric_cols:
        stats_df = df.select(numeric_cols).describe().toPandas().set_index("summary").T
        st.dataframe(stats_df)
    else:
        st.info("Aucune colonne numérique détectée pour résumé statistique.")
