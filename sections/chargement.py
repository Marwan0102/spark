import streamlit as st

def render(df):
    st.title("Chargement des Données")
    st.subheader("Schéma du DataFrame")
    st.text(df._jdf.schema().treeString())

    st.subheader("Aperçu des données")
    st.dataframe(df.limit(20).toPandas())
