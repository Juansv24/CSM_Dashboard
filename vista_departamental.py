import streamlit as st
import pandas as pd
import plotly.express as px
from google_drive_client import consultar_datos_filtrados


def obtener_todos_los_departamentos_territorio() -> pd.DataFrame:
    """
    Obtiene lista de departamentos únicos con datos tipo_territorio = 'Departamento'

    Returns:
        DataFrame con departamentos disponibles
    """
    try:
        conn = st.session_state.get('duckdb_conn')
        if not conn:
            return pd.DataFrame()

        query = """
            SELECT DISTINCT 
                dpto_cdpmp,
                dpto as Departamento
            FROM datos_principales 
            WHERE tipo_territorio = 'Departamento'
            ORDER BY dpto
        """

        return conn.execute(query).df()

    except Exception as e:
        st.error(f"Error obteniendo departamentos: {str(e)}")
        return pd.DataFrame()


def obtener_ranking_departamentos(umbral_similitud: float,
                                  solo_politica_publica: bool = True,
                                  top_n: int = None) -> pd.DataFrame:
    """
    Genera ranking de departamentos por número de recomendaciones implementadas

    Args:
        umbral_similitud: Similitud mínima
        solo_politica_publica: Si filtrar solo política pública
        top_n: Número máximo de resultados

    Returns:
        DataFrame ordenado por ranking
    """
    try:
        conn = st.session_state.get('duckdb_conn')
        if not conn:
            return pd.DataFrame()

        where_conditions = [
            f"sentence_similarity >= {umbral_similitud}",
            "tipo_territorio = 'Departamento'"
        ]

        if solo_politica_publica:
            where_conditions.append(
                "(predicted_class = 'Incluida' OR "
                "(predicted_class = 'Excluida' AND prediction_confidence < 0.8))"
            )

        where_clause = " AND ".join(where_conditions)
        limit_clause = f"LIMIT {top_n}" if top_n else ""

        query = f"""
            SELECT 
                dpto_cdpmp,
                dpto as Departamento,
                COUNT(DISTINCT recommendation_code) as Recomendaciones_Implementadas,
                COUNT(*) as Total_Oraciones,
                AVG(sentence_similarity) as Similitud_Promedio,
                COUNT(CASE WHEN recommendation_priority = 1 THEN 1 END) as Prioritarias_Implementadas,
                ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT recommendation_code) DESC) as Ranking
            FROM datos_principales 
            WHERE {where_clause}
            GROUP BY dpto_cdpmp, dpto
            ORDER BY Recomendaciones_Implementadas DESC
            {limit_clause}
        """

        return conn.execute(query).df()

    except Exception as e:
        st.error(f"Error generando ranking departamental: {str(e)}")
        return pd.DataFrame()


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


def render_ficha_departamental():
    """Renderiza la vista departamental con filtros y análisis detallado"""

    st.sidebar.markdown("### 🔧 Filtros Departamentales")

    # Obtener todos los departamentos disponibles
    todos_departamentos_df = obtener_todos_los_departamentos_territorio()

    if todos_departamentos_df.empty:
        st.error("No se pudieron cargar los departamentos")
        return

    # Crear listas para selectbox
    departamentos_lista = ['Todos'] + todos_departamentos_df['Departamento'].tolist()

    # Filtro departamento
    selected_department = st.sidebar.selectbox(
        "Departamento:",
        options=departamentos_lista,
        index=1 if len(departamentos_lista) > 1 else 0
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

    # Obtener datos filtrados con tipo_territorio = 'Departamento'
    conn = st.session_state.get('duckdb_conn')
    if not conn:
        st.error("No hay conexión a la base de datos")
        return

    # Construir query con filtro de tipo_territorio
    where_conditions = [
        f"sentence_similarity >= {sentence_threshold}",
        "tipo_territorio = 'Departamento'"
    ]

    if include_policy_only:
        where_conditions.append(
            "(predicted_class = 'Incluida' OR "
            "(predicted_class = 'Excluida' AND prediction_confidence < 0.8))"
        )

    if selected_department != 'Todos':
        departamento_escaped = selected_department.replace("'", "''")
        where_conditions.append(f"dpto = '{departamento_escaped}'")

    where_clause = " AND ".join(where_conditions)

    query = f"""
        SELECT * FROM datos_principales 
        WHERE {where_clause}
        ORDER BY sentence_similarity DESC
    """

    datos_filtrados = conn.execute(query).df()
    high_quality_sentences = datos_filtrados[datos_filtrados['sentence_similarity'] >= sentence_threshold]

    # Validación para departamento sin datos
    if selected_department != 'Todos' and high_quality_sentences.empty:
        st.warning(f"""
        ⚠️ **Sin datos para mostrar**

        El departamento **{selected_department}** no tiene información disponible 
        con el umbral de similitud actual ({sentence_threshold:.2f}).

        **Opciones:**
        - Reduzca el umbral de similitud en el panel izquierdo
        - Desactive el filtro "Solo secciones de política pública" 
        - Seleccione un departamento diferente
        """)
        return

    # Renderizar según selección
    if selected_department != 'Todos':
        _render_vista_departamento_especifico(
            selected_department,
            sentence_threshold,
            include_policy_only,
            datos_filtrados,
            high_quality_sentences
        )
    else:
        _render_vista_comparativa_departamental(sentence_threshold, datos_filtrados)


def _render_vista_departamento_especifico(departamento, sentence_threshold,
                                          include_policy_only, datos_departamento, high_quality_sentences):
    """
    Renderiza vista específica de un departamento

    Args:
        departamento: Nombre del departamento
        sentence_threshold: Umbral de similitud
        include_policy_only: Si filtrar solo política pública
        datos_departamento: Datos del departamento
        high_quality_sentences: Oraciones de alta calidad
    """

    if datos_departamento.empty:
        st.warning(f"No se encontraron datos para {departamento}")
        return

    # Encabezado
    st.markdown(f"""
    <div style="background: linear-gradient(90deg, #1f77b4 0%, #17a2b8 100%); 
                color: white; 
                padding: 2rem; 
                border-radius: 15px; 
                margin-bottom: 2rem;
                box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
        <h1 style="margin: 0.5rem 0 0 0; font-size: 3rem; font-weight: 700;">{departamento}</h1>
        <p style="margin: 0.5rem 0 0 0; font-size: 1.5rem; opacity: 0.9;">Plan de Desarrollo Departamental</p>
    </div>
    """, unsafe_allow_html=True)

    # Análisis de implementación
    _render_analisis_implementacion_departamento(
        datos_departamento,
        high_quality_sentences,
        departamento,
        sentence_threshold,
        include_policy_only
    )

    st.markdown("---")

    # Análisis detallado de recomendaciones
    _render_analisis_detallado_recomendaciones(high_quality_sentences, departamento)

    st.markdown("---")

    # Diccionario de recomendaciones
    _render_diccionario_recomendaciones(datos_departamento, departamento)


def _render_vista_comparativa_departamental(sentence_threshold, datos_comparativos):
    """
    Renderiza vista comparativa de múltiples departamentos

    Args:
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
        <h1>📊 Vista Comparativa Departamental</h1>
        <p>Todos los departamentos</p>
    </div>
    """, unsafe_allow_html=True)

    if datos_comparativos.empty:
        st.warning("No hay datos disponibles con los filtros actuales")
        return

    # Métricas resumen
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Departamentos", datos_comparativos['dpto'].nunique())
    with col2:
        st.metric("Recomendaciones", datos_comparativos['recommendation_code'].nunique())
    with col3:
        avg_similarity = datos_comparativos['sentence_similarity'].mean()
        st.metric("Similitud Promedio", f"{avg_similarity:.3f}")

    st.info("💡 Seleccione un departamento específico para ver el reporte detallado")


def _render_analisis_implementacion_departamento(datos_departamento, high_quality_sentences,
                                                 departamento, sentence_threshold, include_policy_only):
    """
    Renderiza análisis de implementación para departamento específico

    Args:
        datos_departamento: Datos del departamento
        high_quality_sentences: Oraciones de alta calidad
        departamento: Nombre del departamento
        sentence_threshold: Umbral de similitud
        include_policy_only: Si filtrar solo política pública
    """

    st.markdown("### 📈 Análisis de Implementación")

    # Calcular métricas
    recomendaciones_implementadas = high_quality_sentences['recommendation_code'].nunique()
    recomendaciones_prioritarias = high_quality_sentences[
        high_quality_sentences['recommendation_priority'] == 1
        ]['recommendation_code'].nunique()

    # Obtener ranking general
    ranking_data = obtener_ranking_departamentos(
        umbral_similitud=sentence_threshold,
        solo_politica_publica=include_policy_only,
        top_n=None
    )

    ranking_position = "N/A"
    total_departamentos = len(ranking_data)

    if not ranking_data.empty:
        depto_rank = ranking_data[ranking_data['Departamento'] == departamento]
        if not depto_rank.empty:
            ranking_position = depto_rank['Ranking'].iloc[0]

    # Mostrar métricas
    col1, col2, col3 = st.columns(3)

    with col1:
        ranking_text = f"{ranking_position}/{total_departamentos}" if ranking_position != "N/A" else "N/A"
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
                    file_name=f"top_5_recomendaciones_{departamento.replace(' ', '_')}.csv",
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
                        file_name=f"implementacion_por_tema_{departamento.replace(' ', '_')}.csv",
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


def _render_analisis_detallado_recomendaciones(high_quality_sentences, departamento):
    """
    Renderiza análisis detallado de recomendaciones con pestañas jerárquicas

    Args:
        high_quality_sentences: Oraciones de alta calidad filtradas
        departamento: Nombre del departamento
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
            key=f"detailed_rec_select_{departamento}",
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
                key=f"hierarchy_tabs_{departamento}"
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

                # Paginación
                coincidencias_por_pagina = 5
                total_coincidencias = len(paragraph_analysis)
                total_paginas = max(1, (total_coincidencias - 1) // coincidencias_por_pagina + 1)

                pagina_key = f'pagina_parrafos_{departamento}_{selected_rec_code}'
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
                                     key=f"prev_parrafos_{departamento}_{selected_rec_code}"):
                            st.session_state[pagina_key] = max(1, pagina_actual - 1)
                            st.rerun()

                    with col_info:
                        st.markdown(f"<center>Página {pagina_actual} de {total_paginas}</center>",
                                    unsafe_allow_html=True)

                    with col_next:
                        if st.button("Siguiente ▶", disabled=(pagina_actual >= total_paginas),
                                     key=f"next_parrafos_{departamento}_{selected_rec_code}"):
                            st.session_state[pagina_key] = min(total_paginas, pagina_actual + 1)
                            st.rerun()

            # PESTAÑA 2: NIVEL DE ORACIÓN
            else:
                st.markdown("**Análisis por Oraciones:**")

                sentence_analysis = rec_data.sort_values('sentence_similarity', ascending=False)

                # Paginación
                coincidencias_por_pagina = 5
                total_coincidencias = len(sentence_analysis)
                total_paginas = max(1, (total_coincidencias - 1) // coincidencias_por_pagina + 1)

                pagina_key = f'pagina_oraciones_{departamento}_{selected_rec_code}'
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
                                     key=f"prev_oraciones_{departamento}_{selected_rec_code}"):
                            st.session_state[pagina_key] = max(1, pagina_actual - 1)
                            st.rerun()

                    with col_info:
                        st.markdown(f"<center>Página {pagina_actual} de {total_paginas}</center>",
                                    unsafe_allow_html=True)

                    with col_next:
                        if st.button("Siguiente ▶", disabled=(pagina_actual >= total_paginas),
                                     key=f"next_oraciones_{departamento}_{selected_rec_code}"):
                            st.session_state[pagina_key] = min(total_paginas, pagina_actual + 1)
                            st.rerun()

    else:
        st.info("No hay recomendaciones disponibles con el filtro actual.")


def _render_diccionario_recomendaciones(datos_departamento, departamento):
    """
    Renderiza diccionario de recomendaciones con vista de tabla optimizada

    Args:
        datos_departamento: Datos del departamento
        departamento: Nombre del departamento
    """

    st.markdown("### 📖 Diccionario de Recomendaciones")

    # Obtener recomendaciones únicas con sus detalles
    recommendations_dict = datos_departamento.groupby('recommendation_code').agg({
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
            key=f"search_dict_{departamento}"
        )

    with col2:
        if 'recommendation_topic' in datos_departamento.columns:
            available_topics = ['Todos'] + sorted(datos_departamento['recommendation_topic'].dropna().unique().tolist())
            selected_topic = st.selectbox(
                "Filtrar por tema:",
                options=available_topics,
                index=0,
                key=f"topic_dict_{departamento}"
            )
        else:
            selected_topic = 'Todos'

    with col3:
        priority_filter = st.selectbox(
            "Prioridad GN:",
            options=['Todos', 'Solo priorizadas', 'Solo no priorizadas'],
            index=0,
            key=f"priority_dict_{departamento}"
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
            key=f"detail_dict_{departamento}"
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
            file_name=f"diccionario_recomendaciones_{departamento.replace(' ', '_')}.csv",
            mime="text/csv; charset=utf-8",
        )
    else:
        st.info("No se encontraron recomendaciones que coincidan con los criterios de búsqueda.")