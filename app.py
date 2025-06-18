import streamlit as st
from data_loader import init_spark, load_data
from sections import accueil, chargement, comparaison, profilage, rapport

spark = init_spark()
df = load_data("openfood.csv", spark)

menu = st.sidebar.selectbox(
    "Navigation",
    ["Accueil", "Chargement des données", "Comparaison formats", "Profilage des données", "Rapport"]
)

if menu == "Accueil":
    accueil.render()
elif menu == "Chargement des données":
    chargement.render(df)
elif menu == "Comparaison formats":
    comparaison.render(df)
elif menu == "Profilage des données":
    profilage.render(df)
elif menu == "Rapport":
    rapport.render(df)
