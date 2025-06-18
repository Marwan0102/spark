import streamlit as st

def render():
    st.title("Projet de Profilage de Données – Open Food Facts")
    st.markdown("""
    Ce projet a pour but d’analyser la qualité et la structure d’un jeu de données issu d’Open Food Facts
    en utilisant **Apache Spark**.

    Les étapes du projet :
    - Chargement du fichier.
    - Comparaison de performances entre formats.
    - Analyse de la qualité des données.
    - Rapport de profilage.
    """)
