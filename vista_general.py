import streamlit as st
import requests
import plotly.express as px
from google_drive_client import (
    obtener_ranking_municipios,
    obtener_top_recomendaciones,
    obtener_municipios_por_recomendacion,
    obtener_estadisticas_departamentales
)


@st.cache_data
def cargar_geojson():
    """Carga datos GeoJSON de Colombia desde URL pública"""
    try:
        url = "https://gist.githubusercontent.com/john-guerra/43c7656821069d00dcbc/raw/be6a6e239cd5b5b803c6e7c2ec405b793a9064dd/Colombia.geo.json"
        response = requests.get(url)
        return response.json()
    except Exception as e:
        st.warning(f"Error cargando GeoJSON departamental: {str(e)}")
        return None


@st.cache_data
def cargar_geojson_municipios():
    """Carga GeoJSON de municipios desde archivo local"""
    try:
        import json

        geojson_path = "Mapa Municipios.geojson"

        with open(geojson_path, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)

        return geojson_data

    except FileNotFoundError:
        st.error("❌ No se encontró el archivo GeoJSON de municipios")
        return None
    except Exception as e:
        st.error(f"Error cargando GeoJSON municipal: {str(e)}")
        return None


def obtener_metadatos_filtrados(umbral_similitud, filtro_pdet, filtro_iica, filtro_mdm):
    """
    Obtiene métricas básicas aplicando filtros

    Args:
        umbral_similitud: Similitud mínima
        filtro_pdet: Filtro PDET
        filtro_iica: Lista categorías IICA
        filtro_mdm: Lista grupos MDM

    Returns:
        Diccionario con estadísticas filtradas
    """
    try:
        conn = st.session_state.get('duckdb_conn')
        if not conn:
            return {}

        where_conditions = [
            f"sentence_similarity >= {umbral_similitud}",
            "tipo_territorio = 'Municipio'"
        ]

        if filtro_pdet == "Solo PDET":
            where_conditions.append("PDET = 1")
        elif filtro_pdet == "Solo No PDET":
            where_conditions.append("PDET = 0")

        if filtro_iica and len(filtro_iica) > 0:
            iica_list = "','".join(filtro_iica)
            where_conditions.append(f"Cat_IICA IN ('{iica_list}')")

        if filtro_mdm and len(filtro_mdm) > 0:
            mdm_list = "','".join(filtro_mdm)
            where_conditions.append(f"Grupo_MDM IN ('{mdm_list}')")

        where_clause = " AND ".join(where_conditions)

        query = f"""
            SELECT 
                COUNT(*) as total_registros,
                COUNT(DISTINCT dpto) as total_departamentos,
                COUNT(DISTINCT mpio_cdpmp) as total_municipios,
                COUNT(DISTINCT recommendation_code) as total_recomendaciones,
                AVG(sentence_similarity) as similitud_promedio
            FROM datos_principales
            WHERE {where_clause}
        """

        resultado = conn.execute(query).fetchone()

        return {
            'total_registros': resultado[0],
            'total_departamentos': resultado[1],
            'total_municipios': resultado[2],
            'total_recomendaciones': resultado[3],
            'similitud_promedio': resultado[4]
        }

    except Exception as e:
        st.error(f"Error obteniendo metadatos filtrados: {str(e)}")
        return {}


def render_vista_general(metadatos, geojson_data=None, dept_data=None, umbral_similitud=0.65):
    """
    Renderiza la vista general del dashboard

    Args:
        metadatos: Diccionario con metadatos básicos
        geojson_data: Datos GeoJSON (opcional, se carga si no existe)
        dept_data: Datos departamentales (opcional, se calcula si no existe)
        umbral_similitud: Umbral de similitud por defecto
    """

    # === FILTROS GLOBALES ===
    st.sidebar.header("🔧 Filtro similitud")

    min_similarity = st.sidebar.slider(
        "Similitud Mínima:",
        min_value=0.5,
        max_value=1.0,
        value=0.65,
        step=0.01
    )

    st.sidebar.markdown("---")
    st.sidebar.subheader("🔍 Filtros socioeconómicos")

    filtro_pdet = st.sidebar.selectbox(
        "Municipios PDET:",
        options=["Todos", "Solo PDET", "Solo No PDET"],
        index=0,
        help="Programa de Desarrollo con Enfoque Territorial"
    )

    filtro_iica = st.sidebar.multiselect(
        "Categoría IICA:",
        options=["Muy Alto", "Alto", "Medio", "Bajo", "Medio Bajo"],
        default=[],
        help="Índice de Incidencia del Conflicto Armado"
    )

    filtro_mdm = st.sidebar.multiselect(
        "Grupo MDM:",
        options=["C", "G1", "G2", "G3", "G4", "G5"],
        default=[],
        help="Capacidades Iniciales - Medición de Desempeño Municipal"
    )

    # === CARGAR DATOS DEPARTAMENTALES ===
    geojson_data = cargar_geojson()
    dept_data = obtener_estadisticas_departamentales(
        min_similarity,
        filtro_pdet=filtro_pdet,
        filtro_iica=filtro_iica,
        filtro_mdm=filtro_mdm
    ) if geojson_data else None

    # === ANÁLISIS GENERAL ===
    st.header("📈 Análisis general")

    metadatos_filtrados = obtener_metadatos_filtrados(
        min_similarity,
        filtro_pdet,
        filtro_iica,
        filtro_mdm
    )

    # Métricas principales
    col1, col2, col3 = st.columns(3)

    with col1:
        total_deptos = metadatos_filtrados.get('total_departamentos', metadatos['total_departamentos'])
        st.metric("Departamentos", total_deptos)

    with col2:
        total_mpios = metadatos_filtrados.get('total_municipios', metadatos['total_municipios'])
        st.metric("Municipios", total_mpios)

    with col3:
        if dept_data is not None and 'Promedio_Recomendaciones' in dept_data.columns:
            promedio_nacional = dept_data['Promedio_Recomendaciones'].mean()
            st.metric(
                "Promedio",
                f"{promedio_nacional:.0f}",
                delta="recomendaciones/municipio"
            )
        else:
            st.metric("Promedio", "N/A")

    # Mapa coroplético
    if geojson_data and dept_data is not None:
        _render_choropleth_map(dept_data, geojson_data, min_similarity)
    else:
        st.warning("No se pudo cargar el mapa de Colombia")

    st.markdown("---")

    # Estadísticas de recomendaciones
    _render_recommendations_stats(
        min_similarity,
        metadatos,
        filtro_pdet,
        filtro_iica,
        filtro_mdm
    )

    st.markdown("---")

    # Análisis de implementación
    _render_implementation_analysis(
        min_similarity,
        filtro_pdet,
        filtro_iica,
        filtro_mdm
    )

    st.markdown("---")

    # Análisis detallado por recomendación
    _render_detailed_analysis(
        min_similarity,
        filtro_pdet,
        filtro_iica,
        filtro_mdm
    )


def _render_choropleth_map(dept_data, geojson_data, min_similarity):
    """
    Renderiza mapa coroplético departamental o municipal

    Args:
        dept_data: DataFrame con datos departamentales
        geojson_data: Datos GeoJSON
        min_similarity: Umbral de similitud aplicado
    """

    # Si hay departamento seleccionado, mostrar mapa municipal
    if st.session_state.get('selected_department_code'):
        _render_municipal_map(st.session_state['selected_department_code'], min_similarity)
        return

    # Calcular rango dinámico para escala de color
    min_valor = dept_data['Promedio_Recomendaciones'].min()
    max_valor = dept_data['Promedio_Recomendaciones'].max()
    rango = max_valor - min_valor
    margen = rango * 0.05
    min_escala = max(0, min_valor - margen)
    max_escala = min(75, max_valor + margen)

    # Crear mapa
    fig_map = px.choropleth(
        dept_data,
        geojson=geojson_data,
        locations='dpto_cdpmp',
        color='Promedio_Recomendaciones',
        color_continuous_scale='viridis',
        range_color=[min_escala, max_escala],
        title='Promedio de recomendaciones mencionadas por municipio',
        hover_name='Departamento',
        hover_data={
            'dpto_cdpmp': False,
            'Promedio_Recomendaciones': False,
            'Municipios': False,
            'Min_Recomendaciones': False,
            'Municipio_Min': False,
            'Max_Recomendaciones': False,
            'Municipio_Max': False
        },
        featureidkey="properties.DPTO",
        custom_data=['Municipios', 'Promedio_Recomendaciones',
                     'Min_Recomendaciones', 'Municipio_Min',
                     'Max_Recomendaciones', 'Municipio_Max']
    )

    fig_map.update_traces(
        hovertemplate='<b>%{hovertext}</b><br><br>' +
                      'Número de Municipios: %{customdata[0]}<br>' +
                      'Promedio recomendaciones: %{customdata[1]:.0f}<br><br>' +
                      '<b>Mínimo:</b> %{customdata[2]:.0f} (%{customdata[3]})<br>' +
                      '<b>Máximo:</b> %{customdata[4]:.0f} (%{customdata[5]})<br>' +
                      '<extra></extra>'
    )

    fig_map.update_geos(
        showframe=False,
        showcoastlines=False,
        projection_type="mercator",
        fitbounds="locations"
    )

    fig_map.update_layout(height=600, margin={"r": 0, "t": 30, "l": 0, "b": 0})
    st.plotly_chart(fig_map, use_container_width=True)

    # Lista de departamentos con botones
    with st.expander("🗺️ Ver detalle municipal por departamento", expanded=False):
        st.markdown("Seleccione un departamento para explorar sus municipios:")

        dept_data_sorted = dept_data.sort_values('Promedio_Recomendaciones', ascending=False)

        cols_per_row = 4
        rows_needed = (len(dept_data_sorted) + cols_per_row - 1) // cols_per_row

        for row in range(rows_needed):
            cols = st.columns(cols_per_row)

            for col_idx in range(cols_per_row):
                dept_idx = row * cols_per_row + col_idx

                if dept_idx < len(dept_data_sorted):
                    dept_row = dept_data_sorted.iloc[dept_idx]
                    dept_name = dept_row['Departamento']
                    dept_code = dept_row['dpto_cdpmp']

                    with cols[col_idx]:
                        if st.button(
                                f"{dept_name}",
                                key=f"btn_dept_{dept_code}",
                                use_container_width=True
                        ):
                            st.session_state['selected_department_code'] = dept_code
                            st.rerun()


def _render_municipal_map(dpto_code, min_similarity):
    """
    Renderiza mapa de municipios de un departamento específico

    Args:
        dpto_code: Código del departamento
        min_similarity: Umbral de similitud
    """

    st.markdown("---")

    # Botón para volver
    col1, col2 = st.columns([1, 5])
    with col1:
        if st.button("◀ Volver", use_container_width=True):
            st.session_state['selected_department_code'] = None
            st.rerun()

    with col2:
        st.subheader("🗺️ Detalle Municipal")

    conn = st.session_state.get('duckdb_conn')
    if not conn:
        return

    # Cargar GeoJSON municipal
    geojson_municipal = cargar_geojson_municipios()
    if not geojson_municipal:
        st.warning("No se pudo cargar el mapa de municipios")
        return

    # Normalizar código del departamento
    dpto_code_normalized = str(dpto_code).zfill(2)

    # Obtener datos municipales
    query = f"""
        SELECT 
            mpio_cdpmp,
            mpio as Municipio,
            dpto as Departamento,
            COUNT(DISTINCT recommendation_code) as Num_Recomendaciones,
            AVG(sentence_similarity) as Similitud_Promedio
        FROM datos_principales
        WHERE dpto_cdpmp = '{dpto_code_normalized}'
        AND sentence_similarity >= {min_similarity}
        AND tipo_territorio = 'Municipio'
        GROUP BY mpio_cdpmp, mpio, dpto
        ORDER BY Num_Recomendaciones DESC
    """

    municipal_data = conn.execute(query).df()

    if municipal_data.empty:
        st.warning("⚠️ No hay datos municipales disponibles")
        return

    # Filtrar GeoJSON
    geojson_filtered = {
        "type": "FeatureCollection",
        "features": [
            feature for feature in geojson_municipal['features']
            if str(feature['properties'].get('DPTO_CCDGO', '')).zfill(2) == dpto_code_normalized
        ]
    }

    if len(geojson_filtered['features']) == 0:
        st.warning("⚠️ No se encontraron geometrías municipales")
        st.dataframe(
            municipal_data[['Municipio', 'Num_Recomendaciones', 'Similitud_Promedio']],
            use_container_width=True,
            hide_index=True
        )
        return

    # Normalizar códigos municipales
    municipal_data['mpio_cdpmp_normalized'] = municipal_data['mpio_cdpmp'].astype(str).str.zfill(5)

    # Crear mapa municipal
    fig_municipal = px.choropleth(
        municipal_data,
        geojson=geojson_filtered,
        locations='mpio_cdpmp_normalized',
        color='Num_Recomendaciones',
        color_continuous_scale='viridis',
        title=f'Recomendaciones por municipio - {municipal_data["Departamento"].iloc[0]}',
        hover_name='Municipio',
        hover_data={
            'mpio_cdpmp_normalized': False,
            'Num_Recomendaciones': True,
            'Similitud_Promedio': ':.3f'
        },
        featureidkey="properties.MPIO_CCNCT",
        labels={
            'Num_Recomendaciones': 'Número de recomendaciones',
            'Similitud_Promedio': 'Similitud promedio'
        }
    )

    fig_municipal.update_geos(
        fitbounds="locations",
        visible=False
    )

    fig_municipal.update_layout(
        height=600,
        margin={"r": 0, "t": 50, "l": 0, "b": 0}
    )

    st.plotly_chart(fig_municipal, use_container_width=True, key="mapa_municipal")

    # Tabla complementaria
    st.dataframe(
        municipal_data[['Municipio', 'Num_Recomendaciones', 'Similitud_Promedio']],
        use_container_width=True,
        hide_index=True
    )


def _render_recommendations_stats(umbral_similitud, metadatos, filtro_pdet, filtro_iica, filtro_mdm):
    """
    Renderiza estadísticas de recomendaciones

    Args:
        umbral_similitud: Umbral de similitud
        metadatos: Metadatos básicos
        filtro_pdet: Filtro PDET
        filtro_iica: Lista categorías IICA
        filtro_mdm: Lista grupos MDM
    """

    st.subheader("📋 Estadísticas de recomendaciones mencionadas a nivel nacional")

    metadatos_filtrados = obtener_metadatos_filtrados(
        umbral_similitud,
        filtro_pdet,
        filtro_iica,
        filtro_mdm
    )

    unique_recommendations = metadatos_filtrados.get('total_recomendaciones', metadatos['total_recomendaciones'])
    total_analyzed = 75

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Recomendaciones mencionadas", unique_recommendations)
    with col2:
        st.metric("Total analizadas", total_analyzed)
    with col3:
        implementation_rate = (unique_recommendations / total_analyzed) * 100
        st.metric("Tasa de mención", f"{implementation_rate:.1f}%")

    st.progress(implementation_rate / 100)


def _render_implementation_analysis(umbral_similitud, filtro_pdet, filtro_iica, filtro_mdm):
    """
    Renderiza análisis de implementación con gráficos

    Args:
        umbral_similitud: Umbral de similitud
        filtro_pdet: Filtro PDET
        filtro_iica: Lista categorías IICA
        filtro_mdm: Lista grupos MDM
    """

    st.subheader("📈 Análisis general de mención")

    col1, col2 = st.columns(2)

    with col1:
        # Top recomendaciones
        top_recs = obtener_top_recomendaciones(
            umbral_similitud=umbral_similitud,
            limite=10,
            filtro_pdet=filtro_pdet,
            filtro_iica=filtro_iica,
            filtro_mdm=filtro_mdm
        )

        if not top_recs.empty:
            fig_top = px.bar(
                top_recs,
                x='Frecuencia_Oraciones',
                y='Codigo',
                orientation='h',
                title='Top 10 recomendaciones por frecuencia de mención',
                labels={
                    'Frecuencia_Oraciones': 'Frecuencia mención',
                    'Codigo': 'Código'
                },
                color='Frecuencia_Oraciones',
                color_continuous_scale='viridis'
            )
            fig_top.update_layout(height=500, showlegend=False, coloraxis_showscale=False)
            fig_top.update_xaxes(title_text="Frecuencia mención")
            fig_top.update_yaxes(title_text="Código de recomendación")
            st.plotly_chart(fig_top, use_container_width=True)

    with col2:
        # Ranking municipios
        ranking_municipios = obtener_ranking_municipios(
            umbral_similitud=umbral_similitud,
            top_n=100,
            filtro_pdet=filtro_pdet,
            filtro_iica=filtro_iica,
            filtro_mdm=filtro_mdm
        )

        if not ranking_municipios.empty:
            avg_implementations = ranking_municipios['Recomendaciones_Implementadas'].mean()
            avg_oraciones = ranking_municipios['Total_Oraciones'].mean()

            fig_scatter = px.scatter(
                ranking_municipios,
                x='Recomendaciones_Implementadas',
                y='Total_Oraciones',
                title='Municipios: Recomendaciones vs frecuencia de menciones',
                labels={
                    'Recomendaciones_Implementadas': 'Recomendaciones mencionadas',
                    'Total_Oraciones': 'Frecuencia de menciones'
                },
                hover_data=['Municipio', 'Departamento']
            )

            fig_scatter.add_vline(x=avg_implementations, line_dash="dot", line_color="red")
            fig_scatter.add_hline(y=avg_oraciones, line_dash="dot", line_color="red")
            fig_scatter.update_xaxes(title_text="Recomendaciones mencionadas")
            fig_scatter.update_yaxes(title_text="Frecuencia de menciones")
            fig_scatter.update_layout(height=500)
            st.plotly_chart(fig_scatter, use_container_width=True)


def _render_detailed_analysis(umbral_similitud, filtro_pdet, filtro_iica, filtro_mdm):
    """
    Renderiza análisis detallado por recomendación con paginación

    Args:
        umbral_similitud: Umbral de similitud
        filtro_pdet: Filtro PDET
        filtro_iica: Lista categorías IICA
        filtro_mdm: Lista grupos MDM
    """

    st.subheader("🔍 Análisis por recomendación")

    # Obtener lista de recomendaciones
    top_recs = obtener_top_recomendaciones(
        umbral_similitud=umbral_similitud,
        limite=50,
        filtro_pdet=filtro_pdet,
        filtro_iica=filtro_iica,
        filtro_mdm=filtro_mdm
    )

    if top_recs.empty:
        st.warning("No hay recomendaciones disponibles")
        return

    # Selector de recomendación
    selected_rec = st.selectbox(
        "Selecciona una recomendación:",
        options=top_recs['Codigo'].tolist(),
        format_func=lambda x: f"{x} - {top_recs[top_recs['Codigo'] == x]['Texto'].iloc[0][:50]}..."
    )

    if selected_rec:
        # Resetear página cuando cambia la recomendación
        pagina_key = f'pagina_{selected_rec}'
        if pagina_key not in st.session_state:
            st.session_state[pagina_key] = 1
            keys_to_remove = [k for k in st.session_state.keys() if k.startswith('pagina_') and k != pagina_key]
            for k in keys_to_remove:
                del st.session_state[k]

        # Info básica de la recomendación
        rec_info = top_recs[top_recs['Codigo'] == selected_rec].iloc[0]

        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Menciones", f"{rec_info['Frecuencia_Oraciones']:,}")
        with col2:
            st.metric("Municipios", f"{rec_info['Municipios_Implementan']:,}")

        # Obtener municipios que implementan esta recomendación
        municipios_impl = obtener_municipios_por_recomendacion(
            selected_rec,
            umbral_similitud,
            limite=100,
            filtro_pdet=filtro_pdet,
            filtro_iica=filtro_iica,
            filtro_mdm=filtro_mdm
        )

        if not municipios_impl.empty:
            # Filtro de búsqueda
            search_term = st.text_input(
                "🔎 Buscar municipio:",
                placeholder="Escriba el nombre del municipio...",
                key=f"search_{selected_rec}"
            )

            # Aplicar filtro
            if search_term:
                mask = municipios_impl['Municipio'].str.contains(search_term, case=False, na=False)
                municipios_filtered = municipios_impl[mask]
                st.session_state[pagina_key] = 1
            else:
                municipios_filtered = municipios_impl

            # Paginación
            municipios_por_pagina = 10
            total_municipios = len(municipios_filtered)
            total_paginas = max(1, (total_municipios - 1) // municipios_por_pagina + 1)

            pagina_actual = st.session_state[pagina_key]

            if pagina_actual > total_paginas:
                st.session_state[pagina_key] = 1
                pagina_actual = 1

            inicio = (pagina_actual - 1) * municipios_por_pagina
            fin = inicio + municipios_por_pagina
            municipios_pagina = municipios_filtered.iloc[inicio:fin]

            st.info(
                f"📊 Mostrando {len(municipios_pagina)} de {total_municipios} municipios (Página {pagina_actual} de {total_paginas})")

            st.markdown("**Municipios que implementan esta recomendación:**")

            # Mostrar municipios con tarjetas
            for idx, (_, row) in enumerate(municipios_pagina.iterrows()):
                ranking_pos = inicio + idx + 1

                st.markdown(f"""
                <div style="background-color: #f8f9fa; padding: 1rem; margin: 0.5rem 0; border-radius: 8px; border-left: 4px solid #007bff;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <strong style="font-size: 1.1rem; color: #333;">#{ranking_pos} {row['Municipio']}</strong>
                            <p style="margin: 0; color: #666; font-size: 0.9rem;">{row['Departamento']}</p>
                            <p style="margin: 0; color: #888; font-size: 0.8rem;">Similitud promedio: {row['Similitud_Promedio']:.3f}</p>
                        </div>
                        <div style="text-align: right;">
                            <span style="background-color: #007bff; color: white; padding: 4px 8px; border-radius: 12px; font-size: 0.9rem;">
                                {row['Frecuencia_Oraciones']} menciones
                            </span>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

            # Controles de paginación
            if total_paginas > 1:
                st.markdown("---")
                col_prev, col_info, col_next = st.columns([1, 2, 1])

                with col_prev:
                    if st.button("◀ Anterior", disabled=(pagina_actual <= 1), key=f"prev_{selected_rec}"):
                        st.session_state[pagina_key] = max(1, pagina_actual - 1)
                        st.rerun()

                with col_info:
                    st.markdown(f"<center>Página {pagina_actual} de {total_paginas}</center>", unsafe_allow_html=True)

                with col_next:
                    if st.button("Siguiente ▶", disabled=(pagina_actual >= total_paginas), key=f"next_{selected_rec}"):
                        st.session_state[pagina_key] = min(total_paginas, pagina_actual + 1)
                        st.rerun()

        else:
            st.info("No se encontraron municipios que implementen esta recomendación.")