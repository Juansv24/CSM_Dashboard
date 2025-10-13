import streamlit as st
from vista_general import render_vista_general
from vista_departamental import render_ficha_departamental
from vista_municipal import render_ficha_municipal
from google_drive_client import (
    conectar_duckdb_parquet,
    obtener_metadatos_basicos
)

st.set_page_config(
    page_title="Dashboard de Similitudes Jerárquicas",
    page_icon="📊",
    layout="wide"
)


def main():
    st.markdown("# 📊 Dashboard de Similitudes Jerárquicas")

    # Inicializar conexión a DuckDB
    if 'duckdb_conn' not in st.session_state:
        conn = conectar_duckdb_parquet()
        st.session_state.duckdb_conn = conn

        if conn is None:
            st.error("❌ No se pudo conectar a la base de datos")
            st.stop()

    # Cargar metadatos básicos
    metadatos = obtener_metadatos_basicos()
    if not metadatos:
        st.error("❌ No se pudieron obtener los metadatos")
        st.stop()

    # Información del sidebar
    st.sidebar.markdown("# Objetivo del dashboard:")
    st.sidebar.markdown(" ")
    st.sidebar.markdown("""
        Sistema que identifica mediante algoritmos de similitud semántica 
        la mención de 75 recomendaciones de la Comisión para el Esclarecimiento 
        de la Verdad (CEV) en los planes de desarrollo territorial (PDT) de 
        1.028 municipios y 33 departamentos en Colombia.
    """)
    st.sidebar.markdown("---")

    # Instrucciones de uso
    with st.expander("ℹ️ ¿Cómo usar este dashboard?", expanded=False):
        st.markdown("""
        ### 📖 ¿Qué es la similitud semántica?

        Es una medida que indica **qué tan parecido es el significado** entre dos textos, 
        independientemente de las palabras utilizadas. El sistema evalúa qué tan parecido
        es el contenido de una recomendación con la oración de un PDT específico.

        Así, podemos identificar cuándo un plan de desarrollo menciona una recomendación 
        incluso si no usa las mismas palabras, pero expresa la misma idea.

        **Escala de interpretación:**
        - 🟢 **0.80-1.00:** La oración menciona prácticamente lo mismo que la recomendación
        - 🟡 **0.65-0.79:** El concepto de la recomendación está claramente presente
        - 🟠 **0.50-0.64:** Hay elementos relacionados pero menos directos

        **Ejemplo práctico del municipio de Leticia, Amazonas:**

        - **Recomendación MCV1:** Diseñar e implementar políticas públicas con enfoque 
          de género para erradicar la discriminación y alcanzar la igualdad de las mujeres 
          en los territorios.
        - **Política pública del PDT de Leticia:** Gestión de acciones y movilización de 
          procesos en marco de la política pública de igualdad y equidad de género para la mujer.
        - **Similitud:** 0.9 - Mencionan prácticamente la misma idea con diferentes palabras.

        ---

        ### ⚙️ Estructura del dashboard

        **Vista General (🌎):**
        - Mapa interactivo con número de recomendaciones implementadas por territorio
        - Gráfico de recomendaciones más mencionadas
        - Distribución de municipios por número de recomendaciones
        - Análisis detallado por recomendación específica

        **Vista Municipal (🏛️):**
        - Información socioeconómica del municipio:
            - Índice de Pobreza Multidimensional (IPM 2018): 0-100, valores altos = mayor pobreza
            - PDET: "Sí" si el municipio es PDET, "No" en caso contrario
            - Categoría IICA: Bajo, Medio Bajo, Medio, Alto, Muy Alto
            - Nivel MDM: C (capitales, mayor capacidad) hasta G5 (menor capacidad)
        - Ranking del municipio según recomendaciones mencionadas
        - Gráficos de recomendaciones y temas más mencionados
        - Análisis detallado por recomendación
        - Diccionario completo de las 75 recomendaciones

        ### ⚙️ Filtros principales

        **Panel izquierdo (Filtros Globales):**
        - **Umbral de similitud:** Ajusta qué tan estricto es el filtro 
          (0.5 = similitud media, más coincidencias | 0.9 = similitud muy alta, menos coincidencias)
          - Recomendado: 0.65 para balance entre cobertura y calidad
        - **Filtros socioeconómicos:** PDET, IICA, IPM, MDM
        """)

    st.markdown("---")

    # Selector de vista
    selected_view = st.segmented_control(
        "Selecciona la vista:",
        ["🌎 General", "🏢 Departamental", "🏛️ Municipal",],
        default="🌎 General"
    )

    # Renderizar vista seleccionada
    if selected_view == "🌎 General":
        render_vista_general(metadatos)
    elif selected_view == "🏛️ Municipal":
        render_ficha_municipal()
    elif selected_view == "🏢 Departamental":
        render_ficha_departamental()


if __name__ == "__main__":
    main()