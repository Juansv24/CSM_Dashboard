import streamlit as st
import pandas as pd
import plotly.express as px
import io
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from google_drive_client import (
    consultar_datos_filtrados,
    obtener_ranking_municipios,
    obtener_todos_los_municipios,
    obtener_todos_los_departamentos
)


def create_variable_dictionary():
    """Crear diccionario de variables del dataset"""
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
            'Indicador numérico de priorización (0=No, 1=Sí)',
            'Texto de la oración del PDD municipal',
            'Similitud semántica entre oración y recomendación (0-1)',
            'Texto completo del párrafo que contiene la oración',
            'Similitud semántica entre párrafo y recomendación (0-1)',
            'Identificador único del párrafo',
            'Número de página del documento donde aparece el texto',
            'Clasificación de ML: Incluida/Excluida como política pública',
            'Confianza del modelo de clasificación (0-1)',
            'Índice de Pobreza Multidimensional 2018',
            'Indicador PDET - Programa de Desarrollo con Enfoque Territorial (0=No, 1=Sí)',
            'Categoría del Índice de Incidencia del Conflicto Armado',
            'Grupo de Capacidades Iniciales - Medición de Desempeño Municipal',
            'Identificador de oración en el documento',
            'Identificador de oración dentro del párrafo'
        ]
    }
    return pd.DataFrame(dictionary)

def create_ranking_data(df, sentence_threshold, include_policy_only):
    """Crear datos de ranking de municipios"""
    ranking_data = df.copy()

    # Aplicar filtro de política si está activado
    if include_policy_only:
        ranking_data = ranking_data[
            (ranking_data['predicted_class'] == 'Incluida') |
            ((ranking_data['predicted_class'] == 'Excluida') & (ranking_data['prediction_confidence'] < 0.8))
            ]

    # Calcular ranking
    ranking_data = ranking_data.groupby(['mpio', 'dpto']).agg({
        'recommendation_code': lambda x: len(set(x[df.loc[x.index, 'sentence_similarity'] >= sentence_threshold])),
        'sentence_similarity': ['count', 'mean'],
        'IPM_2018': 'first',
        'PDET': 'first',
        'Cat_IICA': 'first',
        'Grupo_MDM': 'first'
    }).reset_index()

    # Aplanar columnas
    ranking_data.columns = ['Municipio', 'Departamento', 'Recomendaciones_Implementadas',
                            'Total_Oraciones', 'Similitud_Promedio', 'IPM_2018', 'PDET',
                            'Cat_IICA', 'Grupo_MDM']

    # Ordenar por recomendaciones implementadas
    ranking_data = ranking_data.sort_values('Recomendaciones_Implementadas', ascending=False)
    ranking_data['Ranking'] = range(1, len(ranking_data) + 1)

    # Reordenar columnas
    ranking_data = ranking_data[['Ranking', 'Municipio', 'Departamento', 'Recomendaciones_Implementadas',
                                 'Total_Oraciones', 'Similitud_Promedio', 'IPM_2018', 'PDET',
                                 'Cat_IICA', 'Grupo_MDM']]

    return ranking_data

def create_excel_file(filtered_data, ranking_data, dictionary_df):
    """Crear archivo Excel con ranking, datos filtrados y diccionario"""
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Pestaña 1: Ranking de municipios
        ranking_data.to_excel(writer, sheet_name='Ranking_Municipios', index=False)

        # Pestaña 2: Datos filtrados
        filtered_data.to_excel(writer, sheet_name='Datos_Filtrados', index=False)

        # Pestaña 3: Diccionario
        dictionary_df.to_excel(writer, sheet_name='Diccionario_Variables', index=False)

        # Formatear hojas
        workbook = writer.book
        header_fill = PatternFill(start_color='1f77b4', end_color='1f77b4', fill_type='solid')
        header_font = Font(color='FFFFFF', bold=True)

        # Formatear todas las hojas
        for sheet_name in ['Ranking_Municipios', 'Datos_Filtrados', 'Diccionario_Variables']:
            worksheet = writer.sheets[sheet_name]
            for cell in worksheet[1]:
                cell.fill = header_fill
                cell.font = header_font
                cell.alignment = Alignment(horizontal='center')

        # Ajustar ancho de columnas específicas
        # Ranking
        ranking_sheet = writer.sheets['Ranking_Municipios']
        ranking_sheet.column_dimensions['A'].width = 25  # Municipio
        ranking_sheet.column_dimensions['B'].width = 20  # Departamento

        # Diccionario
        dict_sheet = writer.sheets['Diccionario_Variables']
        dict_sheet.column_dimensions['A'].width = 25
        dict_sheet.column_dimensions['B'].width = 80

    output.seek(0)
    return output

def to_csv_utf8_bom(df):
    """Convertir DataFrame a CSV con codificación UTF-8 BOM"""
    # Crear CSV como string
    csv_string = df.to_csv(index=False, encoding='utf-8')
    # Agregar BOM (Byte Order Mark) para UTF-8
    csv_bytes = '\ufeff' + csv_string
    return csv_bytes.encode('utf-8')

def mostrar_paginacion_coincidencias(rec_code, level="oraciones"):
    """Mostrar controles de paginación para coincidencias de una recomendación específica"""
    pagina_key = f'pagina_actual_coincidencias_{rec_code}_{level}'
    total_paginas_key = f'total_paginas_coincidencias_{rec_code}_{level}'

    pagina_actual = st.session_state.get(pagina_key, 1)
    total_paginas = st.session_state.get(total_paginas_key, 1)

    if total_paginas <= 1:
        return

    st.markdown("---")

    # Crear columnas para centrar la paginación
    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        # Crear botones de paginación
        cols = st.columns([1, 1, 3, 1, 1])

        # Botón anterior
        with cols[0]:
            if st.button("◀", disabled=(pagina_actual <= 1), key=f"prev_page_{level}_{rec_code}"):
                st.session_state[pagina_key] = max(1, pagina_actual - 1)
                st.rerun()

        # Números de página como botones
        with cols[2]:
            # Mostrar páginas como botones (máximo 5 páginas visibles)
            paginas_a_mostrar = []

            if total_paginas <= 5:
                paginas_a_mostrar = list(range(1, total_paginas + 1))
            else:
                if pagina_actual <= 3:
                    paginas_a_mostrar = [1, 2, 3, 4, 5]
                elif pagina_actual >= total_paginas - 2:
                    paginas_a_mostrar = list(range(total_paginas - 4, total_paginas + 1))
                else:
                    paginas_a_mostrar = list(range(pagina_actual - 2, pagina_actual + 3))

            # Crear mini-columnas para cada número de página
            mini_cols = st.columns(len(paginas_a_mostrar))

            for i, pagina in enumerate(paginas_a_mostrar):
                with mini_cols[i]:
                    if pagina == pagina_actual:
                        st.markdown(
                            f"<div style='background: #007bff; color: white; text-align: center; padding: 4px; border-radius: 4px; margin: 2px;'>{pagina}</div>",
                            unsafe_allow_html=True)
                    else:
                        if st.button(str(pagina), key=f"page_{level}_{rec_code}_{pagina}"):
                            st.session_state[pagina_key] = pagina
                            st.rerun()

        # Botón siguiente
        with cols[4]:
            if st.button("▶", disabled=(pagina_actual >= total_paginas), key=f"next_page_{level}_{rec_code}"):
                st.session_state[pagina_key] = min(total_paginas, pagina_actual + 1)
                st.rerun()

def render_ficha_municipal():
    """Vista municipal optimizada"""

    st.sidebar.markdown("### 🔧 Filtros Municipales")

    # Obtener TODOS los territorios de la muestra (sin filtros)
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

    # Filtro municipio - SIEMPRE mostrar todos los municipios disponibles
    if selected_department == 'Todos':
        municipios_disponibles = todos_municipios_df['Municipio'].unique().tolist()
    else:
        # Filtrar municipios del departamento seleccionado
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

    # Umbral local
    sentence_threshold = st.sidebar.slider(
        "Umbral de Similitud:",
        min_value=0.0,
        max_value=1.0,
        value=0.6,
        step=0.05,
        help="Filtro para mostrar solo oraciones con similitud igual o superior al valor seleccionado"
    )

    # Filtro de política
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

    high_quality_sentences = datos_filtrados[datos_filtrados['sentence_similarity'] >= sentence_threshold]

    # VALIDACIÓN PARA MUNICIPIO SIN DATOS
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

def _render_vista_municipio_especifico(municipio, departamento, sentence_threshold, include_policy_only,
                                       datos_municipio, high_quality_sentences):
    """Vista específica de municipio"""

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

    # Descripción de funcionalidad
    with st.expander("ℹ️ Acerca de este Dashboard", expanded=False):
        st.markdown("""
            ### 📋 Ficha Municipal - Análisis de Implementación de Recomendaciones

            Este dashboard permite analizar el nivel de mención de las recomendaciones de política pública 
            en los planes de desarrollo (PDD) de los municipios y departamentos de Colombia mediante técnicas de similitud semántica.

            **Funcionalidades principales:**
            - **Filtrado:** Seleccione departamento y municipio para análisis específico
            - **Umbral de similitud:** Ajuste el nivel mínimo de coincidencia entre recomendaciones y texto municipal (entre más alto más calidad tendrán las coincidencias)
            - **Ranking:** Compare el desempeño relativo entre municipios
            - **Análisis detallado:** Explore recomendaciones específicas y oraciones relacionadas

            **Cómo interpretar los resultados:**
            - Mayor similitud (0 - 1) indica una coincidencia de mejor calidad entre el texto del PDD y las recomendaciones
            - El ranking se basa en el número total de recomendaciones implementadas
            """)

    # Información básica
    st.markdown("### 📊 Información Básica")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        ipm_raw = muni_info.get('IPM_2018', 'N/A')

        # Use pandas to_numeric for robust conversion
        try:
            ipm_numeric = pd.to_numeric(ipm_raw, errors='coerce')
            if pd.notna(ipm_numeric):
                ipm = f"{ipm_numeric:.2f}"
            else:
                ipm = 'N/A'
        except:
            ipm = 'N/A'

        st.markdown(f"""
            <div style="background-color: #f8f9fa; padding: 1rem; border-radius: 10px; text-align: center; border-left: 4px solid #6c757d;">
                <h4 style="margin: 0; color: #6c757d;">IPM 2018 
                    <span title="Índice de Pobreza Multidimensional 2018: Mide la pobreza considerando múltiples dimensiones como educación, salud, trabajo, etc. Valores más altos indican mayor pobreza." 
                          style="cursor: help; color: #007bff;">ⓘ</span>
                </h4>
                <h3 style="margin: 0.5rem 0 0 0; color: #333;">{ipm}</h3>
            </div>
            """, unsafe_allow_html=True)

    with col2:
        pdet = muni_info.get('PDET', 'N/A')
        pdet_color = "#28a745" if pdet == 1 else "#dc3545" if pdet == 0 else "#6c757d"
        pdet_text = "SÍ" if pdet == 1 else "NO" if pdet == 0 else "N/A"
        st.markdown(f"""
            <div style="background-color: #f8f9fa; padding: 1rem; border-radius: 10px; text-align: center; border-left: 4px solid {pdet_color};">
                <h4 style="margin: 0; color: #6c757d;">PDET 
                    <span title="Programa de Desarrollo con Enfoque Territorial: Indica si el municipio está incluido en este programa para territorios afectados por el conflicto." 
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
                    <span title="Índice de Incidencia del Conflicto Armado: Clasifica los municipios en 5 categorías según el nivel de incidencia del conflicto armado, desde baja hasta muy alta incidencia. Incluye variables como acciones armadas, desplazamiento forzado, cultivos de coca y homicidios." 
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
                    <span title="Grupo de Capacidades Iniciales de la Medición de Desempeño Municipal: Clasifica los municipios en 6 categorías según sus capacidades económicas, urbanas y de recursos. La clasificación va desde C (capitales) con las mayores capacidades, seguida de G1, G2, G3, G4, hasta G5 con las menores capacidades iniciales." 
                          style="cursor: help; color: #007bff;">ⓘ</span>
                </h4>
                <h3 style="margin: 0.5rem 0 0 0; color: #333;">{grupo_mdm if pd.notna(grupo_mdm) else 'N/A'}</h3>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("---")

    # Análisis de implementación
    _render_analisis_implementacion_municipio(datos_municipio, high_quality_sentences, municipio, sentence_threshold,
                                              include_policy_only)

    st.markdown("---")

    # Análisis detallado de recomendaciones
    _render_analisis_detallado_recomendaciones(high_quality_sentences)

    st.markdown("---")

    # Diccionario de recomendaciones
    _render_diccionario_recomendaciones(datos_municipio, municipio, include_policy_only)


def _render_vista_comparativa(selected_department, sentence_threshold, datos_comparativos):
    """Vista comparativa de múltiples municipios"""

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


def _render_analisis_implementacion_municipio(datos_municipio, high_quality_sentences, municipio, sentence_threshold,
                                              include_policy_only):
    """Análisis de implementación para municipio específico"""

    st.markdown("### 📈 Análisis de Implementación")

    # Calcular métricas
    recomendaciones_implementadas = high_quality_sentences['recommendation_code'].nunique()

    # Recomendaciones prioritarias implementadas
    recomendaciones_prioritarias = high_quality_sentences[
        high_quality_sentences['recommendation_priority'] == 1
        ]['recommendation_code'].nunique()

    # Obtener ranking general
    ranking_data = obtener_ranking_municipios(
        umbral_similitud=sentence_threshold,
        solo_politica_publica=include_policy_only,
        top_n=None
    )

    ranking_position = "N/A"
    total_municipios = len(ranking_data)

    if not ranking_data.empty:
        municipio_rank = ranking_data[ranking_data['Municipio'] == municipio]
        if not municipio_rank.empty:
            ranking_position = municipio_rank['Ranking'].iloc[0]

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

        # Header con botón de descarga
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
                    use_container_width=True
                )

            # Gráfico
            fig_freq = px.bar(
                freq_analysis,
                x='Frecuencia',
                y='Código',
                orientation='h',
                title='Número de Oraciones por Recomendación',
                labels={'Frecuencia': 'Número de Oraciones', 'Código': 'Código de Recomendación'},
                color='Frecuencia',
                color_continuous_scale='blues',
                hover_data={'Texto': True, 'Frecuencia': True}
            )
            fig_freq.update_layout(
                height=400,
                showlegend=False,
                coloraxis_showscale=False
            )
            st.plotly_chart(fig_freq, use_container_width=True)

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
                        use_container_width=True
                    )

                # Gráfico
                fig_heatmap = px.bar(
                    topic_analysis,
                    x='Recomendaciones_Implementadas',
                    y='Tema',
                    orientation='h',
                    title='Recomendaciones Implementadas por Tema',
                    labels={'Recomendaciones_Implementadas': 'Número de Recomendaciones', 'Tema': ''},
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
                st.plotly_chart(fig_heatmap, use_container_width=True)


def _render_analisis_detallado_recomendaciones(high_quality_sentences):
    """Análisis detallado de recomendaciones con pestañas jerárquicas"""

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
                ["📝 Párrafos", "💬 Oraciones"],
                selection_mode="single",
                default="💬 Oraciones",
                key="hierarchy_tabs"
            )

            # PESTAÑA 1: NIVEL DE PÁRRAFO
            if tab == "📝 Párrafos":
                st.markdown("**Análisis por Párrafos:**")

                # Agrupar por párrafo y calcular similitud a nivel de párrafo
                paragraph_analysis = rec_data.groupby(['paragraph_id', 'paragraph_text']).agg({
                    'paragraph_similarity': 'first',
                    'page_number': 'first',
                    'sentence_similarity': ['count', 'mean', 'max'],
                    'predicted_class': lambda x: x.mode()[0] if not x.empty else 'N/A'
                }).reset_index()

                paragraph_analysis.columns = ['ID_Párrafo', 'Texto_Párrafo', 'Similitud_Párrafo', 'Página',
                                              'Num_Oraciones', 'Similitud_Prom', 'Similitud_Max',
                                              'Clasificación_ML']
                paragraph_analysis = paragraph_analysis.sort_values('Similitud_Prom', ascending=False)

                # PAGINACIÓN PARA PÁRRAFOS
                coincidencias_por_pagina = 5
                total_coincidencias = len(paragraph_analysis)
                total_paginas = max(1, (total_coincidencias - 1) // coincidencias_por_pagina + 1)

                # Inicializar página actual para párrafos de esta recomendación
                pagina_key = f'pagina_actual_coincidencias_{selected_rec_code}_parrafos'
                if pagina_key not in st.session_state:
                    st.session_state[pagina_key] = 1

                # Validar que la página actual no exceda el total
                if st.session_state[pagina_key] > total_paginas:
                    st.session_state[pagina_key] = 1

                pagina_actual = st.session_state[pagina_key]

                # Aplicar paginación
                inicio = (pagina_actual - 1) * coincidencias_por_pagina
                fin = inicio + coincidencias_por_pagina
                paragraph_analysis_paginado = paragraph_analysis.iloc[inicio:fin]

                # Guardar en session state para controles de paginación
                st.session_state[f'total_paginas_coincidencias_{selected_rec_code}_parrafos'] = total_paginas

                # Mostrar información de paginación
                st.write(
                    f"📋 Mostrando {len(paragraph_analysis_paginado)} de {total_coincidencias} párrafos (Página {pagina_actual} de {total_paginas})")

                # Mostrar párrafos paginados
                for idx, row in paragraph_analysis_paginado.iterrows():
                    with st.expander(
                            f"Párrafo {row['ID_Párrafo']} - Similitud Promedio: {row['Similitud_Prom']:.3f}",
                            expanded=idx == paragraph_analysis_paginado.index[0]):  # Solo el primero expandido
                        col1, col2 = st.columns([3, 1])

                        with col1:
                            st.write("**Contenido del Párrafo:**")
                            # Truncar texto muy largo
                            para_text = row['Texto_Párrafo'][:800] + "..." if len(
                                row['Texto_Párrafo']) > 800 else row['Texto_Párrafo']
                            st.write(para_text)

                        with col2:
                            st.write("**Métricas:**")
                            st.write(f"**ID Página:** {row['Página']}")
                            st.write(f"**ID Párrafo:** {row['ID_Párrafo']}")
                            st.write(f"**Similitud Párrafo:** {row['Similitud_Párrafo']:.3f}")

                # Mostrar controles de paginación para párrafos
                mostrar_paginacion_coincidencias(selected_rec_code, "parrafos")

            # PESTAÑA 2: NIVEL DE ORACIÓN
            else:  # "💬 Oraciones"
                st.markdown("**Análisis por Oraciones:**")

                sentence_analysis = rec_data.sort_values('sentence_similarity', ascending=False)

                # PAGINACIÓN PARA ORACIONES
                coincidencias_por_pagina = 5
                total_coincidencias = len(sentence_analysis)
                total_paginas = max(1, (total_coincidencias - 1) // coincidencias_por_pagina + 1)

                # Inicializar página actual para oraciones de esta recomendación
                pagina_key = f'pagina_actual_coincidencias_{selected_rec_code}_oraciones'
                if pagina_key not in st.session_state:
                    st.session_state[pagina_key] = 1

                # Validar que la página actual no exceda el total
                if st.session_state[pagina_key] > total_paginas:
                    st.session_state[pagina_key] = 1

                pagina_actual = st.session_state[pagina_key]

                # Aplicar paginación
                inicio = (pagina_actual - 1) * coincidencias_por_pagina
                fin = inicio + coincidencias_por_pagina
                sentence_analysis_paginado = sentence_analysis.iloc[inicio:fin]

                # Mostrar información de paginación
                st.write(
                    f"📋 Mostrando {len(sentence_analysis_paginado)} de {total_coincidencias} oraciones (Página {pagina_actual} de {total_paginas})")

                # Mostrar oraciones paginadas
                for idx, (_, row) in enumerate(sentence_analysis_paginado.iterrows()):
                    sentence_id = row.get('sentence_id_paragraph', f'S{idx + 1}')

                    with st.expander(f"Oración {sentence_id} - Similitud: {row['sentence_similarity']:.3f}",
                                     expanded=idx == 0):  # Solo la primera expandida
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

                # Mostrar controles de paginación para oraciones
                mostrar_paginacion_coincidencias(selected_rec_code, "oraciones")

    else:
        st.info("No hay recomendaciones disponibles con el filtro actual.")

def _render_diccionario_recomendaciones(datos_municipio, municipio, include_policy_only):
    """Diccionario de recomendaciones"""

    st.markdown("### 📖 Diccionario de Recomendaciones")

    # Usar datos del municipio específico
    dict_data = datos_municipio

    # Obtener recomendaciones únicas con sus detalles
    recommendations_dict = dict_data.groupby('recommendation_code').agg({
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
            "🔍 Buscar recomendación:",
            placeholder="Ingrese código o palabras clave...",
            help="Busque por código de recomendación o palabras en el texto",
            key=f"search_dict_{municipio}"
        )

    with col2:
        if 'recommendation_topic' in dict_data.columns:
            available_topics = ['Todos'] + sorted(
                dict_data['recommendation_topic'].dropna().unique().tolist())
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

    # PAGINACIÓN
    recomendaciones_por_pagina = 5
    total_recomendaciones = len(filtered_dict)
    total_paginas = max(1, (total_recomendaciones - 1) // recomendaciones_por_pagina + 1)

    # Clave única para esta sección
    pagina_key = f'pagina_dict_recs_{municipio.replace(" ", "_")}'
    if pagina_key not in st.session_state:
        st.session_state[pagina_key] = 1

    # Resetear página cuando cambian los filtros
    if search_term or selected_topic != 'Todos' or priority_filter != 'Todos':
        # Solo resetear si hay filtros activos y cambió el contenido
        if pagina_key in st.session_state and st.session_state[pagina_key] > total_paginas:
            st.session_state[pagina_key] = 1

    # Validar página actual
    if st.session_state[pagina_key] > total_paginas:
        st.session_state[pagina_key] = 1

    pagina_actual = st.session_state[pagina_key]

    # Aplicar paginación
    inicio = (pagina_actual - 1) * recomendaciones_por_pagina
    fin = inicio + recomendaciones_por_pagina
    filtered_dict_paginado = filtered_dict.iloc[inicio:fin]

    # Mostrar contador de resultados con información de paginación
    st.markdown(f"**Mostrando {len(filtered_dict_paginado)} de {total_recomendaciones} recomendaciones (Página {pagina_actual} de {total_paginas})**")

    # Mostrar recomendaciones
    if not filtered_dict_paginado.empty:
        for idx, row in filtered_dict_paginado.iterrows():
            with st.expander(f"**{row['Código']}** - {row['Texto'][:80]}...", expanded=False):
                col1, col2 = st.columns([3, 1])

                with col1:
                    st.markdown("**Descripción completa:**")
                    st.write(row['Texto'])

                    if pd.notna(row['Tema']):
                        st.markdown(f"**Tema:** {row['Tema']}")

                with col2:
                    st.markdown("**Información:**")
                    st.write(f"**Código:** {row['Código']}")

                    if pd.notna(row['Priorizado_GN']):
                        priority_text = "Sí" if row['Priorizado_GN'] == 1 else "No"
                        priority_color = "🔴" if row['Priorizado_GN'] == 1 else "⚪"
                        st.write(f"**Priorizado por GN:** {priority_color} {priority_text}")

                    st.markdown("**Estadísticas:**")
                    st.write(f"**Total menciones:** {row['Total_Menciones']}")
                    st.write(f"**Similitud promedio:** {row['Similitud_Promedio']:.3f}")
                    st.write(f"**Similitud máxima:** {row['Similitud_Máxima']:.3f}")

        # Controles de paginación
        if total_paginas > 1:
            st.markdown("---")
            col_prev, col_info, col_next = st.columns([1, 2, 1])

            with col_prev:
                if st.button("◀ Anterior", disabled=(pagina_actual <= 1), key=f"prev_dict_{municipio}"):
                    st.session_state[pagina_key] = max(1, pagina_actual - 1)
                    st.rerun()

            with col_info:
                st.markdown(f"<center>Página {pagina_actual} de {total_paginas}</center>", unsafe_allow_html=True)

            with col_next:
                if st.button("Siguiente ▶", disabled=(pagina_actual >= total_paginas), key=f"next_dict_{municipio}"):
                    st.session_state[pagina_key] = min(total_paginas, pagina_actual + 1)
                    st.rerun()

    else:
        st.info("No se encontraron recomendaciones que coincidan con los criterios de búsqueda.")