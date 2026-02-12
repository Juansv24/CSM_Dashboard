import streamlit as st
import pandas as pd
import plotly.express as px
import io
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from data_client import (
    consultar_datos_filtrados,
    obtener_ranking_municipio_especifico,
    obtener_todos_los_municipios,
    obtener_todos_los_departamentos
)


def create_variable_dictionary():
    """
    Crea diccionario de variables del dataset

    Returns:
        DataFrame con nombres y descripciones de variables
    """
    dictionary = {
        'Variable': [
            'mpio', 'dpto', 'recommendation_code', 'recommendation_text',
            'recommendation_topic', 'recommendation_priority', 'sentence_text',
            'sentence_similarity', 'paragraph_text', 'paragraph_similarity',
            'paragraph_id', 'page_number', 'predicted_class', 'prediction_confidence',
            'IPM_2018', 'PDET', 'Cat_IICA', 'Grupo_MDM', 'sentence_id', 'sentence_id_paragraph'
        ],
        'Descripción': [
            'Nombre del municipio',
            'Nombre del departamento',
            'Código único de la recomendación',
            'Texto completo de la recomendación',
            'Tema o categoría de la recomendación',
            'Indicador de priorización (0=No, 1=Sí)',
            'Texto de la oración del PDT municipal',
            'Similitud semántica entre oración y recomendación (0-1)',
            'Texto completo del párrafo que contiene la oración',
            'Similitud semántica entre párrafo y recomendación (0-1)',
            'Identificador único del párrafo',
            'Número de página del documento',
            'Clasificación ML: Incluida/Excluida como política pública',
            'Confianza del modelo de clasificación (0-1)',
            'Índice de Pobreza Multidimensional 2018',
            'Indicador PDET (0=No, 1=Sí)',
            'Categoría del Índice de Incidencia del Conflicto Armado',
            'Grupo de Capacidades Iniciales MDM',
            'Identificador de oración en el documento',
            'Identificador de oración dentro del párrafo'
        ]
    }
    return pd.DataFrame(dictionary)


def to_csv_utf8_bom(df):
    """
    Convierte DataFrame a CSV con codificación UTF-8 BOM

    Args:
        df: DataFrame a convertir

    Returns:
        Bytes del CSV con BOM UTF-8
    """
    csv_string = df.to_csv(index=False, encoding='utf-8')
    csv_bytes = '\ufeff' + csv_string
    return csv_bytes.encode('utf-8')


def render_ficha_municipal():
    """Renderiza la vista municipal con filtros y análisis detallado"""

    st.sidebar.markdown("### 🔧 Filtros Municipales")

    # Obtener todos los territorios disponibles
    todos_municipios_df = obtener_todos_los_municipios()
    todos_departamentos_df = obtener_todos_los_departamentos()

    if todos_departamentos_df.empty:
        st.error("No se pudieron cargar los territorios")
        return

    # Crear listas para selectbox
    departamentos_lista = ['Todos'] + todos_departamentos_df['Departamento'].tolist()

    # Filtro departamento
    selected_department = st.sidebar.selectbox(
        "Departamento:",
        options=departamentos_lista,
        index=1 if len(departamentos_lista) > 1 else 0
    )

    # Filtro municipio - siempre mostrar todos los disponibles
    if selected_department == 'Todos':
        municipios_disponibles = todos_municipios_df['Municipio'].unique().tolist()
    else:
        dpto_municipios = todos_municipios_df[
            todos_municipios_df['Departamento'] == selected_department
            ]['Municipio'].tolist()
        municipios_disponibles = dpto_municipios

    municipios_lista = ['Todos'] + sorted(municipios_disponibles)

    selected_municipality = st.sidebar.selectbox(
        "Municipio:",
        options=municipios_lista,
        index=1 if len(municipios_lista) > 1 else 0
    )

    # Umbral de similitud
    sentence_threshold = st.sidebar.slider(
        "Umbral de Similitud:",
        min_value=0.0,
        max_value=1.0,
        value=0.65,
        step=0.05,
        help="Filtro para mostrar solo oraciones con similitud igual o superior"
    )

    # Filtro de política pública
    include_policy_only = st.sidebar.checkbox(
        "Solo secciones de política pública",
        value=True,
        help="Filtrar para incluir solo contenido clasificado como política pública"
    )

    # Obtener datos filtrados
    datos_filtrados = consultar_datos_filtrados(
        umbral_similitud=sentence_threshold,
        departamento=selected_department if selected_department != 'Todos' else None,
        municipio=selected_municipality if selected_municipality != 'Todos' else None,
        solo_politica_publica=include_policy_only
    )

    # SQL already filters by sentence_similarity >= threshold
    high_quality_sentences = datos_filtrados

    # Validación para municipio sin datos
    if selected_municipality != 'Todos' and high_quality_sentences.empty:
        st.warning(f"""
        ⚠️ **Sin datos para mostrar**

        El municipio **{selected_municipality}** ({selected_department}) no tiene información 
        disponible con el umbral de similitud actual ({sentence_threshold:.2f}).

        **Opciones:**
        - Reduzca el umbral de similitud en el panel izquierdo
        - Desactive el filtro "Solo secciones de política pública" 
        - Seleccione un municipio diferente
        """)
        return

    # Renderizar según selección
    if selected_municipality != 'Todos':
        _render_vista_municipio_especifico(
            selected_municipality,
            selected_department,
            sentence_threshold,
            include_policy_only,
            datos_filtrados,
            high_quality_sentences
        )
    else:
        _render_vista_comparativa(selected_department, sentence_threshold, datos_filtrados)


def _render_vista_municipio_especifico(municipio, departamento, sentence_threshold,
                                       include_policy_only, datos_municipio, high_quality_sentences):
    """
    Renderiza vista específica de un municipio

    Args:
        municipio: Nombre del municipio
        departamento: Nombre del departamento
        sentence_threshold: Umbral de similitud
        include_policy_only: Si filtrar solo política pública
        datos_municipio: Datos del municipio
        high_quality_sentences: Oraciones de alta calidad
    """

    if datos_municipio.empty:
        st.warning(f"No se encontraron datos para {municipio}")
        return

    # Encabezado
    muni_info = datos_municipio.iloc[0]
    st.markdown(f"""
    <div style="background: linear-gradient(90deg, #1f77b4 0%, #17a2b8 100%); 
                color: white; 
                padding: 2rem; 
                border-radius: 15px; 
                margin-bottom: 2rem;
                box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
        <h1 style="margin: 0.5rem 0 0 0; font-size: 3rem; font-weight: 700;">{municipio}</h1>
        <p style="margin: 0.5rem 0 0 0; font-size: 1.5rem; opacity: 0.9;">{departamento}, Colombia</p>
    </div>
    """, unsafe_allow_html=True)

    # Información básica
    st.markdown("### 📊 Información Básica")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        ipm_raw = muni_info.get('IPM_2018', 'N/A')
        try:
            ipm_numeric = pd.to_numeric(ipm_raw, errors='coerce')
            ipm = f"{ipm_numeric:.2f}" if pd.notna(ipm_numeric) else 'N/A'
        except:
            ipm = 'N/A'

        st.markdown(f"""
        <div style="background-color: #f8f9fa; padding: 1rem; border-radius: 10px; text-align: center; border-left: 4px solid #6c757d;">
            <h4 style="margin: 0; color: #6c757d;">IPM 2018 
                <span title="Índice de Pobreza Multidimensional 2018: Valores más altos indican mayor pobreza." 
                      style="cursor: help; color: #007bff;">ⓘ</span>
            </h4>
            <h3 style="margin: 0.5rem 0 0 0; color: #333;">{ipm}</h3>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        pdet = muni_info.get('PDET', 'N/A')
        pdet_color = "#28a745" if pdet == 1 else "#dc3545" if pdet == 0 else "#6c757d"
        pdet_text = "Sí" if pdet == 1 else "NO" if pdet == 0 else "N/A"
        st.markdown(f"""
        <div style="background-color: #f8f9fa; padding: 1rem; border-radius: 10px; text-align: center; border-left: 4px solid {pdet_color};">
            <h4 style="margin: 0; color: #6c757d;">PDET 
                <span title="Programa de Desarrollo con Enfoque Territorial" 
                      style="cursor: help; color: #007bff;">ⓘ</span>
            </h4>
            <h3 style="margin: 0.5rem 0 0 0; color: {pdet_color};">{pdet_text}</h3>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        cat_iica = muni_info.get('Cat_IICA', 'N/A')
        st.markdown(f"""
        <div style="background-color: #f8f9fa; padding: 1rem; border-radius: 10px; text-align: center; border-left: 4px solid #17a2b8;">
            <h4 style="margin: 0; color: #6c757d;">Categoría IICA 
                <span title="Índice de Incidencia del Conflicto Armado: Bajo, Medio, Alto, Muy Alto" 
                      style="cursor: help; color: #007bff;">ⓘ</span>
            </h4>
            <h3 style="margin: 0.5rem 0 0 0; color: #333;">{cat_iica if pd.notna(cat_iica) else 'N/A'}</h3>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        grupo_mdm = muni_info.get('Grupo_MDM', 'N/A')
        st.markdown(f"""
        <div style="background-color: #f8f9fa; padding: 1rem; border-radius: 10px; text-align: center; border-left: 4px solid #ffc107;">
            <h4 style="margin: 0; color: #6c757d;">Grupo MDM 
                <span title="Capacidades Iniciales: C (mayor) hasta G5 (menor)" 
                      style="cursor: help; color: #007bff;">ⓘ</span>
            </h4>
            <h3 style="margin: 0.5rem 0 0 0; color: #333;">{grupo_mdm if pd.notna(grupo_mdm) else 'N/A'}</h3>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    # Análisis de implementación
    _render_analisis_implementacion_municipio(
        datos_municipio,
        high_quality_sentences,
        municipio,
        sentence_threshold,
        include_policy_only
    )

    st.markdown("---")

    # Análisis detallado de recomendaciones
    _render_analisis_detallado_recomendaciones(high_quality_sentences)

    st.markdown("---")

    # Diccionario de recomendaciones
    _render_diccionario_recomendaciones(datos_municipio, municipio, include_policy_only)


def _render_vista_comparativa(selected_department, sentence_threshold, datos_comparativos):
    """
    Renderiza vista comparativa de múltiples municipios

    Args:
        selected_department: Departamento seleccionado
        sentence_threshold: Umbral de similitud
        datos_comparativos: Datos para comparación
    """

    st.markdown(f"""
    <div style="background: linear-gradient(90deg, #6c757d 0%, #495057 100%); 
                color: white; 
                padding: 2rem; 
                border-radius: 15px; 
                margin-bottom: 2rem; 
                text-align: center;">
        <h1>📊 Vista Comparativa</h1>
        <p>{f"Departamento: {selected_department}" if selected_department != 'Todos' else "Todos los municipios"}</p>
    </div>
    """, unsafe_allow_html=True)

    if datos_comparativos.empty:
        st.warning("No hay datos disponibles con los filtros actuales")
        return

    # Métricas resumen
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Municipios", datos_comparativos['mpio'].nunique())
    with col2:
        st.metric("Departamentos", datos_comparativos['dpto'].nunique())
    with col3:
        st.metric("Recomendaciones", datos_comparativos['recommendation_code'].nunique())
    with col4:
        avg_similarity = datos_comparativos['sentence_similarity'].mean()
        st.metric("Similitud Promedio", f"{avg_similarity:.3f}")

    st.info("💡 Seleccione un municipio específico para ver el reporte detallado")


def _render_analisis_implementacion_municipio(datos_municipio, high_quality_sentences,
                                              municipio, sentence_threshold, include_policy_only):
    """
    Renderiza análisis de implementación para municipio específico

    Args:
        datos_municipio: Datos del municipio
        high_quality_sentences: Oraciones de alta calidad
        municipio: Nombre del municipio
        sentence_threshold: Umbral de similitud
        include_policy_only: Si filtrar solo política pública
    """

    st.markdown("### 📈 Análisis de Implementación")

    # Calcular métricas
    recomendaciones_implementadas = high_quality_sentences['recommendation_code'].nunique()
    recomendaciones_prioritarias = high_quality_sentences[
        high_quality_sentences['recommendation_priority'] == 1
        ]['recommendation_code'].nunique()

    # Obtener targeted ranking (no loading all municipalities)
    ranking_info = obtener_ranking_municipio_especifico(
        municipio=municipio,
        umbral_similitud=sentence_threshold,
        solo_politica_publica=include_policy_only
    )

    ranking_position = ranking_info['ranking_position']
    total_municipios = ranking_info['total_municipios']

    # Mostrar métricas
    col1, col2, col3 = st.columns(3)

    with col1:
        ranking_text = f"{ranking_position}/{total_municipios}" if ranking_position != "N/A" else "N/A"
        st.markdown(f"""
        <div style="background-color: #fff3e0; padding: 1.5rem; border-radius: 10px; text-align: center;">
            <h2 style="margin: 0; color: #EF6C00; font-size: 2.5rem;">{ranking_text}</h2>
            <p style="margin: 0.5rem 0 0 0; color: #EF6C00; font-weight: 500;">Ranking</p>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        recs_text = f"{recomendaciones_implementadas}/75"
        st.markdown(f"""
        <div style="background-color: #e3f2fd; padding: 1.5rem; border-radius: 10px; text-align: center;">
            <h2 style="margin: 0; color: #1976d2; font-size: 2.5rem;">{recs_text}</h2>
            <p style="margin: 0.5rem 0 0 0; color: #1976d2; font-weight: 500;">Recomendaciones Implementadas</p>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown(f"""
        <div style="background-color: #e8f5e8; padding: 1.5rem; border-radius: 10px; text-align: center;">
            <h2 style="margin: 0; color: #388e3c; font-size: 2.5rem;">{recomendaciones_prioritarias}/45</h2>
            <p style="margin: 0.5rem 0 0 0; color: #388e3c; font-weight: 500;">Prioritarias Implementadas</p>
        </div>
        """, unsafe_allow_html=True)

    # Top 5 Recomendaciones por Frecuencia
    if not high_quality_sentences.empty:
        st.markdown(" ")
        st.markdown(" ")

        col_header, col_download = st.columns([4, 1])
        with col_header:
            st.markdown("### Top 5 Recomendaciones más Frecuentes")

        freq_analysis = high_quality_sentences.groupby('recommendation_code').agg({
            'sentence_similarity': 'count',
            'recommendation_text': 'first'
        }).reset_index()
        freq_analysis.columns = ['Código', 'Frecuencia', 'Texto']
        freq_analysis = freq_analysis.sort_values('Frecuencia', ascending=False).head(5)

        if not freq_analysis.empty:
            with col_download:
                csv_freq = to_csv_utf8_bom(freq_analysis)
                st.download_button(
                    label="📄 Descargar",
                    data=csv_freq,
                    file_name="top_5_recomendaciones_frecuentes.csv",
                    mime="text/csv; charset=utf-8",
                    help="Descargar datos del gráfico",
                    width="stretch"
                )

            fig_freq = px.bar(
                freq_analysis,
                x='Frecuencia',
                y='Código',
                orientation='h',
                title='Número de menciones por recomendación',
                labels={'Frecuencia': 'Número de menciones', 'Código': 'Código de Recomendación'},
                color='Frecuencia',
                color_continuous_scale='blues',
                hover_data={'Texto': True, 'Frecuencia': True}
            )
            fig_freq.update_layout(height=400, showlegend=False, coloraxis_showscale=False)
            st.plotly_chart(fig_freq, width="stretch")

        # Implementación por Tema
        if 'recommendation_topic' in high_quality_sentences.columns:
            col_header2, col_download2 = st.columns([4, 1])
            with col_header2:
                st.markdown("#### Implementación por Tema")

            topic_analysis = high_quality_sentences.groupby('recommendation_topic')[
                'recommendation_code'].nunique().reset_index()
            topic_analysis.columns = ['Tema', 'Recomendaciones_Implementadas']
            topic_analysis = topic_analysis.sort_values('Recomendaciones_Implementadas', ascending=False)

            if not topic_analysis.empty:
                with col_download2:
                    csv_topics = to_csv_utf8_bom(topic_analysis)
                    st.download_button(
                        label="📄 Descargar",
                        data=csv_topics,
                        file_name="implementacion_por_tema.csv",
                        mime="text/csv; charset=utf-8",
                        help="Descargar datos del gráfico",
                        width="stretch"
                    )

                fig_heatmap = px.bar(
                    topic_analysis,
                    x='Recomendaciones_Implementadas',
                    y='Tema',
                    orientation='h',
                    title='Recomendaciones mencionadas al menos una vez por tema',
                    labels={'Recomendaciones_Implementadas': 'Número de recomendaciones', 'Tema': ''},
                    color='Recomendaciones_Implementadas',
                    color_continuous_scale='viridis'
                )
                fig_heatmap.update_layout(
                    height=400,
                    showlegend=False,
                    yaxis={'categoryorder': 'total ascending'},
                    margin=dict(l=150, r=50, t=80, b=50),
                    coloraxis_showscale=False
                )
                st.plotly_chart(fig_heatmap, width="stretch")


def _render_analisis_detallado_recomendaciones(high_quality_sentences):
    """
    Renderiza análisis detallado de recomendaciones con pestañas jerárquicas

    Args:
        high_quality_sentences: Oraciones de alta calidad filtradas
    """

    st.markdown("### 🔍 Análisis detallado de recomendaciones")

    if not high_quality_sentences.empty:
        # Selector de recomendación
        available_recommendations = high_quality_sentences['recommendation_code'].unique().tolist()

        selected_rec_code = st.selectbox(
            "Seleccione una recomendación:",
            options=available_recommendations,
            format_func=lambda
                x: f"{x} - {high_quality_sentences[high_quality_sentences['recommendation_code'] == x]['recommendation_text'].iloc[0][:60]}...",
            key="detailed_rec_select",
            label_visibility="collapsed"
        )

        if selected_rec_code:
            rec_data = high_quality_sentences[
                high_quality_sentences['recommendation_code'] == selected_rec_code].copy()

            # Mostrar texto de la recomendación
            rec_text = rec_data['recommendation_text'].iloc[0]
            st.markdown("**Texto de la Recomendación:**")
            st.info(rec_text)

            # Pestañas de navegación jerárquica
            tab = st.segmented_control(
                "Nivel de análisis:",
                ["📄 Párrafos", "💬 Oraciones"],
                selection_mode="single",
                default="💬 Oraciones",
                key="hierarchy_tabs"
            )

            # PESTAÑA 1: NIVEL DE PÁRRAFO
            if tab == "📄 Párrafos":
                st.markdown("**Análisis por Párrafos:**")

                paragraph_analysis = rec_data.groupby(['paragraph_id', 'paragraph_text']).agg({
                    'paragraph_similarity': 'first',
                    'page_number': 'first',
                    'sentence_similarity': ['count', 'mean', 'max'],
                    'predicted_class': lambda x: x.mode()[0] if not x.empty else 'N/A'
                }).reset_index()

                paragraph_analysis.columns = ['ID_Párrafo', 'Texto_Párrafo', 'Similitud_Párrafo', 'Página',
                                              'Num_Oraciones', 'Similitud_Prom', 'Similitud_Max', 'Clasificación_ML']
                paragraph_analysis = paragraph_analysis.sort_values('Similitud_Prom', ascending=False)

                # Paginación para párrafos
                coincidencias_por_pagina = 5
                total_coincidencias = len(paragraph_analysis)
                total_paginas = max(1, (total_coincidencias - 1) // coincidencias_por_pagina + 1)

                pagina_key = f'pagina_actual_coincidencias_{selected_rec_code}_parrafos'
                if pagina_key not in st.session_state:
                    st.session_state[pagina_key] = 1

                if st.session_state[pagina_key] > total_paginas:
                    st.session_state[pagina_key] = 1

                pagina_actual = st.session_state[pagina_key]

                inicio = (pagina_actual - 1) * coincidencias_por_pagina
                fin = inicio + coincidencias_por_pagina
                paragraph_analysis_paginado = paragraph_analysis.iloc[inicio:fin]

                st.write(
                    f"📋 Mostrando {len(paragraph_analysis_paginado)} de {total_coincidencias} párrafos (Página {pagina_actual} de {total_paginas})")

                for idx, row in paragraph_analysis_paginado.iterrows():
                    with st.expander(
                            f"Párrafo {row['ID_Párrafo']} - Similitud Promedio: {row['Similitud_Prom']:.3f}",
                            expanded=idx == paragraph_analysis_paginado.index[0]
                    ):
                        col1, col2 = st.columns([3, 1])

                        with col1:
                            st.write("**Contenido del Párrafo:**")
                            para_text = row['Texto_Párrafo'][:800] + "..." if len(row['Texto_Párrafo']) > 800 else row[
                                'Texto_Párrafo']
                            st.write(para_text)

                        with col2:
                            st.write("**Métricas:**")
                            st.write(f"**ID Página:** {row['Página']}")
                            st.write(f"**ID Párrafo:** {row['ID_Párrafo']}")
                            st.write(f"**Similitud Párrafo:** {row['Similitud_Párrafo']:.3f}")

                # Controles de paginación
                if total_paginas > 1:
                    st.markdown("---")
                    col_prev, col_info, col_next = st.columns([1, 2, 1])

                    with col_prev:
                        if st.button("◀ Anterior", disabled=(pagina_actual <= 1),
                                     key=f"prev_page_parrafos_{selected_rec_code}"):
                            st.session_state[pagina_key] = max(1, pagina_actual - 1)
                            st.rerun()

                    with col_info:
                        st.markdown(f"<center>Página {pagina_actual} de {total_paginas}</center>",
                                    unsafe_allow_html=True)

                    with col_next:
                        if st.button("Siguiente ▶", disabled=(pagina_actual >= total_paginas),
                                     key=f"next_page_parrafos_{selected_rec_code}"):
                            st.session_state[pagina_key] = min(total_paginas, pagina_actual + 1)
                            st.rerun()

            # PESTAÑA 2: NIVEL DE ORACIÓN
            else:
                st.markdown("**Análisis por Oraciones:**")

                sentence_analysis = rec_data.sort_values('sentence_similarity', ascending=False)

                # Paginación para oraciones
                coincidencias_por_pagina = 5
                total_coincidencias = len(sentence_analysis)
                total_paginas = max(1, (total_coincidencias - 1) // coincidencias_por_pagina + 1)

                pagina_key = f'pagina_actual_coincidencias_{selected_rec_code}_oraciones'
                if pagina_key not in st.session_state:
                    st.session_state[pagina_key] = 1

                if st.session_state[pagina_key] > total_paginas:
                    st.session_state[pagina_key] = 1

                pagina_actual = st.session_state[pagina_key]

                inicio = (pagina_actual - 1) * coincidencias_por_pagina
                fin = inicio + coincidencias_por_pagina
                sentence_analysis_paginado = sentence_analysis.iloc[inicio:fin]

                st.write(
                    f"📋 Mostrando {len(sentence_analysis_paginado)} de {total_coincidencias} oraciones (Página {pagina_actual} de {total_paginas})")

                for idx, (_, row) in enumerate(sentence_analysis_paginado.iterrows()):
                    sentence_id = row.get('sentence_id_paragraph', f'S{idx + 1}')

                    with st.expander(f"Oración {sentence_id} - Similitud: {row['sentence_similarity']:.3f}",
                                     expanded=idx == 0):
                        col1, col2 = st.columns([3, 1])

                        with col1:
                            st.write("**Contenido:**")
                            st.write(row['sentence_text'])

                        with col2:
                            st.write("**Métricas:**")
                            if 'sentence_id' in row and pd.notna(row['sentence_id']):
                                st.write(f"**ID Oración:** {row['sentence_id']}")
                            st.write(f"**ID Página:** {row['page_number']}")
                            st.write(f"**ID Párrafo:** {row.get('paragraph_id', 'N/A')}")
                            st.write(f"**Similitud Oración:** {row['sentence_similarity']:.3f}")
                            st.write(f"**Clasificación ML:** {row['predicted_class']}")

                # Controles de paginación
                if total_paginas > 1:
                    st.markdown("---")
                    col_prev, col_info, col_next = st.columns([1, 2, 1])

                    with col_prev:
                        if st.button("◀ Anterior", disabled=(pagina_actual <= 1),
                                     key=f"prev_page_oraciones_{selected_rec_code}"):
                            st.session_state[pagina_key] = max(1, pagina_actual - 1)
                            st.rerun()

                    with col_info:
                        st.markdown(f"<center>Página {pagina_actual} de {total_paginas}</center>",
                                    unsafe_allow_html=True)

                    with col_next:
                        if st.button("Siguiente ▶", disabled=(pagina_actual >= total_paginas),
                                     key=f"next_page_oraciones_{selected_rec_code}"):
                            st.session_state[pagina_key] = min(total_paginas, pagina_actual + 1)
                            st.rerun()

    else:
        st.info("No hay recomendaciones disponibles con el filtro actual.")


def _render_diccionario_recomendaciones(datos_municipio, municipio, include_policy_only):
    """
    Renderiza diccionario de recomendaciones con vista de tabla optimizada

    Args:
        datos_municipio: Datos del municipio
        municipio: Nombre del municipio
        include_policy_only: Si filtrar solo política pública
    """

    st.markdown("### 📖 Diccionario de Recomendaciones")

    # Obtener recomendaciones únicas con sus detalles
    recommendations_dict = datos_municipio.groupby('recommendation_code').agg({
        'recommendation_text': 'first',
        'recommendation_topic': 'first',
        'recommendation_priority': 'first',
        'sentence_similarity': ['count', 'mean', 'max']
    }).reset_index()

    recommendations_dict.columns = ['Código', 'Texto', 'Tema', 'Priorizado_GN', 'Total_Menciones',
                                    'Similitud_Promedio', 'Similitud_Máxima']
    recommendations_dict = recommendations_dict.sort_values('Código')

    # Opciones de búsqueda y filtro
    col1, col2, col3 = st.columns([2, 1, 1])

    with col1:
        search_term = st.text_input(
            "🔎 Buscar recomendación:",
            placeholder="Ingrese código o palabras clave...",
            help="Busque por código de recomendación o palabras en el texto",
            key=f"search_dict_{municipio}"
        )

    with col2:
        if 'recommendation_topic' in datos_municipio.columns:
            available_topics = ['Todos'] + sorted(datos_municipio['recommendation_topic'].dropna().unique().tolist())
            selected_topic = st.selectbox(
                "Filtrar por tema:",
                options=available_topics,
                index=0,
                key=f"topic_dict_{municipio}"
            )
        else:
            selected_topic = 'Todos'

    with col3:
        priority_filter = st.selectbox(
            "Prioridad GN:",
            options=['Todos', 'Solo priorizadas', 'Solo no priorizadas'],
            index=0,
            key=f"priority_dict_{municipio}"
        )

    # Aplicar filtros
    filtered_dict = recommendations_dict.copy()

    if search_term:
        mask = (
                filtered_dict['Código'].str.contains(search_term, case=False, na=False) |
                filtered_dict['Texto'].str.contains(search_term, case=False, na=False)
        )
        filtered_dict = filtered_dict[mask]

    if selected_topic != 'Todos':
        filtered_dict = filtered_dict[filtered_dict['Tema'] == selected_topic]

    if priority_filter == 'Solo priorizadas':
        filtered_dict = filtered_dict[filtered_dict['Priorizado_GN'] == 1]
    elif priority_filter == 'Solo no priorizadas':
        filtered_dict = filtered_dict[filtered_dict['Priorizado_GN'] == 0]

    # Preparar columnas para visualización
    filtered_dict['Priorizado'] = filtered_dict['Priorizado_GN'].apply(
        lambda x: '🔴 Sí' if x == 1 else '⚪ No' if x == 0 else 'N/A'
    )

    filtered_dict['Texto_Corto'] = filtered_dict['Texto'].apply(
        lambda x: x[:100] + '...' if len(x) > 100 else x
    )

    filtered_dict['Similitud_Promedio'] = filtered_dict['Similitud_Promedio'].round(3)
    filtered_dict['Similitud_Máxima'] = filtered_dict['Similitud_Máxima'].round(3)

    # Mostrar contador de resultados
    st.markdown(f"**Total de recomendaciones encontradas: {len(filtered_dict)}**")

    # Mostrar tabla optimizada
    if not filtered_dict.empty:
        display_columns = ['Código', 'Texto_Corto', 'Tema', 'Priorizado',
                           'Total_Menciones', 'Similitud_Promedio', 'Similitud_Máxima']

        display_df = filtered_dict[display_columns].copy()
        display_df.columns = ['Código', 'Descripción', 'Tema', 'Prioritaria',
                              'Menciones', 'Sim. Prom.', 'Sim. Máx.']

        st.dataframe(
            display_df,
            width="stretch",
            height=400,
            hide_index=True,
            column_config={
                "Código": st.column_config.TextColumn("Código", width="small"),
                "Descripción": st.column_config.TextColumn("Descripción", width="large"),
                "Tema": st.column_config.TextColumn("Tema", width="medium"),
                "Prioritaria": st.column_config.TextColumn("Prioritaria", width="small"),
                "Menciones": st.column_config.NumberColumn("Menciones", width="small"),
                "Sim. Prom.": st.column_config.NumberColumn("Sim. Prom.", format="%.3f", width="small"),
                "Sim. Máx.": st.column_config.NumberColumn("Sim. Máx.", format="%.3f", width="small"),
            }
        )

        # Opción para expandir detalles de una recomendación
        st.markdown("---")
        st.markdown("**💡 Ver detalles completos de una recomendación:**")

        selected_code = st.selectbox(
            "Seleccione un código:",
            options=[''] + filtered_dict['Código'].tolist(),
            format_func=lambda x: f"{x}" if x else "-- Seleccione --",
            key=f"detail_dict_{municipio}"
        )

        if selected_code:
            detail_row = filtered_dict[filtered_dict['Código'] == selected_code].iloc[0]

            with st.container():
                st.markdown(f"### {detail_row['Código']}")

                col1, col2 = st.columns([3, 1])

                with col1:
                    st.markdown("**Descripción completa:**")
                    st.write(detail_row['Texto'])

                    if pd.notna(detail_row['Tema']):
                        st.markdown(f"**Tema:** {detail_row['Tema']}")

                with col2:
                    st.markdown("**Información:**")
                    st.write(f"**Código:** {detail_row['Código']}")
                    st.write(f"**Priorizado por GN:** {detail_row['Priorizado']}")

                    st.markdown("**Estadísticas:**")
                    st.write(f"**Total menciones:** {detail_row['Total_Menciones']}")
                    st.write(f"**Similitud promedio:** {detail_row['Similitud_Promedio']:.3f}")
                    st.write(f"**Similitud máxima:** {detail_row['Similitud_Máxima']:.3f}")

        # Opción de descarga
        csv_data = to_csv_utf8_bom(filtered_dict[['Código', 'Texto', 'Tema', 'Priorizado_GN',
                                                  'Total_Menciones', 'Similitud_Promedio', 'Similitud_Máxima']])
        st.download_button(
            label="📥 Descargar diccionario completo (CSV)",
            data=csv_data,
            file_name=f"diccionario_recomendaciones_{municipio.replace(' ', '_')}.csv",
            mime="text/csv; charset=utf-8",
        )
    else:
        st.info("No se encontraron recomendaciones que coincidan con los criterios de búsqueda.")