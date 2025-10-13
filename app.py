import streamlit as st
from general_view import render_general_view
from municipal_view import render_ficha_municipal
from google_drive_client import (
    conectar_duckdb_parquet,
    obtener_metadatos_basicos
)

st.set_page_config(
    page_title="Similitudes Jerárquicas Dashboard",
    page_icon="📊",
    layout="wide"
)

def main():

    st.markdown("""
            # 📊 Dashboard de Similitudes Jerárquicas
                   """)

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

    st.sidebar.markdown("""
        # Objetivo del dashboard:
               """)

    st.sidebar.markdown(" ")

    st.sidebar.markdown("""Sistema que, por medio de algoritmos de similitud semántica, identifica la mención de 75 recomendaciones de la Comisión para el Esclarecimiento de la Verdad (CEV)
     en los planes de desarrollo territorial (PDT) de 1.028 municipios y 33 departamentos en Colombia.
    """)

    st.sidebar.markdown("---")

    # Instrucciones de uso

    with st.expander("ℹ️ ¿Cómo usar este dashboard?", expanded=False):

        st.markdown("""
         
                    ### 📖 ¿Qué es la similitud semántica?
        
                    Es una medida que indica **qué tan parecido es el significado** entre dos textos, 
                    independientemente de las palabras utilizadas. Para este caso, el sistema evalúa qué tan parecido
                    es el contenido de una recomendación en particular con la oración de un PDT específico.
                    
                    Así, podemos identificar cuándo un plan de desarrollo menciona 
                    una recomendación incluso si no usa las mismas palabras, pero expresa la misma idea.
                    
                    **Escala de interpretación:**
                    - 🟢 **0.80-1.00:** La oración identificada menciona prácticamente lo mismo que la recomendación
                    - 🟡 **0.65-0.79:** El concepto de la recomendación está claramente presente en la oración
                    - 🟠 **0.50-0.64:** Hay elementos relacionados pero menos directos en la oración identificada
                    
                    **Ejemplo práctico del municipio de Leticia Amazonas:**
                    
                    - **Recomendación MCV1:** Diseñar e implementar políticas públicas con enfoque de género para erradicar la discriminación y alcanzar la igualdad de las mujeres en los territorios.
                    - **Política pública del PDT de Leticia, Amazonas:** Gestión de acciones y movilización de procesos en marco de la política pública de igualdad y equidad de género para la mujer.
                    - **Similitud:** 0.9. Mencionan prácticamente la misma idea con diferentes palabras.
                    ---
        
                   ### ⚙️ Estructura del dashboard

                   **Vista General (🌎):**
                   - Mapa interactivo mostrando número de recomendaciones implementadas a nivel municipal y departamental en Colombia.
                   - Gráfico de barras con las recomendaciones más mencionadas.
                   - Distribución de municipios por número de recomendaciones mencionadas y por frecuencia de mención. 
                   - Análisis por recomendación: Qué municipios mencionan la recomendación seleccionada ordenados por número de menciones

                   **Vista Municipal (🏛️):**
                   - Información socioeconómica del municipio:
                        - Índice de pobreza multidimensional (IPM 2018): Toma valores entre 0 y 100, donde valores más altos indican mayor incidencia de pobreza.
                        - Identificador si el municipio es PDET: Toma el valor de "Sí"  si el municipio es PDET y "No" de lo contrario.
                        - Categoría del Índice de Incidencia del Conflicto Armado (IICA): Puede tomar 5 valores según la incidencia del conflicto: Bajo, Medio Bajo, Medio, Alto y Muy Alto.
                        - Nivel de capcidades iniciales MDM): La clasificación va desde C (capitales) con las mayores capacidades, seguida de G1, G2, G3, G4, hasta G5 con las menores capacidades iniciales.
                   
                   - Ranking del municipipio según las recomendaciones que menciona en su PDT.
                   - Gráfic de barras que explora las recomendaciones más mencionadas y los temas más mencionados.
                   - Análisis detallado por recomendación
                   - Diccionario completo de las 75 recomendaciones

                   ### ⚙️ Filtros principales

                   **Panel izquierdo (Filtros Globales):**
                   - **Umbral de similitud:** Ajusta qué tan estricto es el filtro de coincidencias (0.5 = similitud media más coincidencias, 0.9 = similitud muy alta menos coincidencias)
                     - Recomendado: 0.65 para balance entre cobertura y calidad.
                   - Filtros socioeconómicos
                
                     
                   """)

    st.markdown("---")

    # Navegación
    selected_view = st.segmented_control(
        "Selecciona la vista:",
        ["🌎 General", "🏛️ Municipal"],
        default="🌎 General"
    )

    # Renderizar vista
    if selected_view == "🌎 General":
        render_general_view(metadatos)
    elif selected_view == "🏛️ Municipal":
        render_ficha_municipal()


if __name__ == "__main__":
    main()