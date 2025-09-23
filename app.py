import streamlit as st
import requests
from general_view import render_general_view
from municipal_view import render_ficha_municipal
from google_drive_client import (
    conectar_duckdb_parquet,
    obtener_metadatos_basicos,
    obtener_estadisticas_departamentales
)

st.set_page_config(
    page_title="Similitudes Jerárquicas Dashboard",
    page_icon="📊",
    layout="wide"
)


@st.cache_data
def cargar_geojson():
    """Cargar datos GeoJSON de Colombia"""
    try:
        url = "https://gist.githubusercontent.com/john-guerra/43c7656821069d00dcbc/raw/be6a6e239cd5b5b803c6e7c2ec405b793a9064dd/Colombia.geo.json"
        response = requests.get(url)
        return response.json()
    except Exception as e:
        st.warning(f"Error cargando GeoJSON: {str(e)}")
        return None


def main():
    st.title("📊 Dashboard de Similitudes Jerárquicas")
    st.markdown("---")

    # Establecer conexión DuckDB
    if 'duckdb_conn' not in st.session_state:
        conn = conectar_duckdb_parquet()
        st.session_state.duckdb_conn = conn

        if conn is None:
            st.error("❌ No se pudo conectar a la base de datos")
            st.stop()

    # Obtener metadatos básicos
    metadatos = obtener_metadatos_basicos()

    if not metadatos:
        st.error("❌ No se pudieron obtener los metadatos")
        st.stop()

    # Filtros globales
    st.sidebar.header("🔧 Filtros Globales")

    min_similarity = st.sidebar.slider(
        "Similitud Mínima:",
        min_value=0.5,
        max_value=1.0,
        value=0.65,
        step=0.01
    )

    # Mostrar estadísticas
    st.info(
        f"🔍 **Datos:** {metadatos['total_registros']:,} registros | "
        f"{metadatos['total_departamentos']} departamentos | "
        f"{metadatos['total_municipios']} municipios | "
        f"{metadatos['total_recomendaciones']} recomendaciones"
    )

    # Preparar datos departamentales
    geojson_data = cargar_geojson()
    dept_data = obtener_estadisticas_departamentales(min_similarity) if geojson_data else None

    # Navegación
    selected_view = st.segmented_control(
        "Selecciona la vista:",
        ["🌎 General", "🏛️ Municipal"],
        default="🌎 General"
    )

    # Renderizar vista
    if selected_view == "🌎 General":
        render_general_view(metadatos, geojson_data, dept_data, min_similarity)
    elif selected_view == "🏛️ Municipal":
        render_ficha_municipal(min_similarity)


if __name__ == "__main__":
    main()