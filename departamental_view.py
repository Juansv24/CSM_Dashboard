import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def render_ficha_departamental(df):
    """Departmental view - placeholder for now"""
    st.header("📍 Vista Departamental")

    if df.empty:
        st.warning("No hay datos departamentales disponibles con el filtro actual.")
        return

    st.info("🚧 Esta vista está en desarrollo. Próximamente tendrás análisis detallado a nivel departamental.")

    # Basic stats as placeholder
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Departamentos", df['dpto'].nunique())

    with col2:
        st.metric("Recomendaciones", df['recommendation_code'].nunique())

    with col3:
        avg_similarity = df['combined_score'].mean()
        st.metric("Similitud Promedio", f"{avg_similarity:.3f}")
    
