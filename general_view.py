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
    """Cargar datos GeoJSON de Colombia"""
    try:
        url = "https://gist.githubusercontent.com/john-guerra/43c7656821069d00dcbc/raw/be6a6e239cd5b5b803c6e7c2ec405b793a9064dd/Colombia.geo.json"
        response = requests.get(url)
        return response.json()
    except Exception as e:
        st.warning(f"Error cargando GeoJSON: {str(e)}")
        return None


def render_general_view(metadatos, geojson_data=None, dept_data=None, umbral_similitud=0.65):
    """Renderizar vista general"""

    # Filtros globales
    st.sidebar.header("🔧 Filtro General")

    min_similarity = st.sidebar.slider(
        "Similitud Mínima:",
        min_value=0.5,
        max_value=1.0,
        value=0.65,
        step=0.01
    )

    # Mostrar estadísticas
    # st.info(
    #   f"🔍 **Datos:** {metadatos['total_registros']:,} registros | "
    #   f"{metadatos['total_departamentos']} departamentos | "
    #   f"{metadatos['total_municipios']} municipios | "
    #   f"{metadatos['total_recomendaciones']} recomendaciones"
    # )

    # Preparar datos departamentales
    geojson_data = cargar_geojson()
    dept_data = obtener_estadisticas_departamentales(min_similarity) if geojson_data else None

    st.header("📈 Análisis general")

    # Métricas principales
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Departamentos", metadatos['total_departamentos'])
    with col2:
        st.metric("Municipios", metadatos['total_municipios'])
    with col3:
        if dept_data is not None:
            total_oraciones = dept_data['Oraciones_Umbral'].sum()
            total_documentos = dept_data['Total_Oraciones'].sum()
            porcentaje_general = (
                    total_oraciones / total_documentos * 100) if total_documentos > 0 else 0
            st.metric(
                "Oraciones sobre Umbral",
                f"{total_oraciones:,}",
                delta=f"{porcentaje_general:.1f}% del total"
            )

    # Mapa coroplético
    if geojson_data and dept_data is not None:
        _render_choropleth_map(dept_data, geojson_data)
    else:
        st.warning("No se pudo cargar el mapa de Colombia")

    st.markdown("---")

    # Estadísticas de recomendaciones
    _render_recommendations_stats(min_similarity, metadatos)

    st.markdown("---")

    # Análisis de implementación
    _render_implementation_analysis(min_similarity)

    st.markdown("---")

    # ANÁLISIS DETALLADO - La sección que buscabas
    _render_detailed_analysis(umbral_similitud)


def _render_choropleth_map(dept_data, geojson_data):
    """Renderizar mapa coroplético"""

    fig_map = px.choropleth(
        dept_data,
        geojson=geojson_data,
        locations='dpto_cdpmp',
        color='Porcentaje_Umbral',
        color_continuous_scale='viridis',
        title='Porcentaje de Oraciones sobre Umbral por Departamento',
        labels={
            'Porcentaje_Umbral': 'Porcentaje de oraciones sobre el umbral (%)',
            'Municipios': 'Número de municipios',
            'Oraciones_Umbral': 'Oraciones sobre el umbral',
            'Total_Oraciones': 'Total de oraciones'
        },
        hover_name='Departamento',
        hover_data={
            'Municipios': True,
            'dpto_cdpmp': False,
            'Oraciones_Umbral': ':,d',
            'Total_Oraciones': ':,d',
            'Porcentaje_Umbral': ':.2f'
        },
        featureidkey="properties.DPTO"
    )

    # Personalizar el hover template para mayor control
    fig_map.update_traces(
        hovertemplate='<b>%{hovertext}</b><br>' +
                      'Número de Municipios: %{customdata[0]}<br>' +
                      'Oraciones sobre Umbral: %{customdata[1]:,}<br>' +
                      'Total de Oraciones: %{customdata[2]:,}<br>' +
                      'Porcentaje sobre Umbral: %{z:.2f}%<br>' +
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


def _render_recommendations_stats(umbral_similitud, metadatos):
    """Estadísticas de recomendaciones"""

    st.subheader("📋 Estadísticas de Recomendaciones")

    unique_recommendations = metadatos['total_recomendaciones']
    total_analyzed = 75

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Recomendaciones Implementadas", unique_recommendations)
    with col2:
        st.metric("Total Analizadas", total_analyzed)
    with col3:
        implementation_rate = (unique_recommendations / total_analyzed) * 100
        st.metric("Tasa de Implementación", f"{implementation_rate:.1f}%")

    st.progress(implementation_rate / 100)


def _render_implementation_analysis(umbral_similitud):
    """Análisis de implementación"""

    st.subheader("📈 Análisis General de Implementación")

    col1, col2 = st.columns(2)

    with col1:
        # Top recomendaciones
        top_recs = obtener_top_recomendaciones(umbral_similitud=umbral_similitud, limite=10)

        if not top_recs.empty:
            fig_top = px.bar(
                top_recs,
                x='Frecuencia_Oraciones',
                y='Codigo',
                orientation='h',
                title='Top 10 Recomendaciones por Frecuencia',
                color='Frecuencia_Oraciones',
                color_continuous_scale='viridis'
            )
            fig_top.update_layout(height=500, showlegend=False, coloraxis_showscale=False)
            st.plotly_chart(fig_top, use_container_width=True)

    with col2:
        # Ranking municipios
        ranking_municipios = obtener_ranking_municipios(umbral_similitud=umbral_similitud, top_n=100)

        if not ranking_municipios.empty:
            avg_implementations = ranking_municipios['Recomendaciones_Implementadas'].mean()
            avg_oraciones = ranking_municipios['Total_Oraciones'].mean()

            fig_scatter = px.scatter(
                ranking_municipios,
                x='Recomendaciones_Implementadas',
                y='Total_Oraciones',
                title='Municipios: Implementaciones vs Oraciones',
                hover_data=['Municipio', 'Departamento']
            )

            fig_scatter.add_vline(x=avg_implementations, line_dash="dot", line_color="red")
            fig_scatter.add_hline(y=avg_oraciones, line_dash="dot", line_color="red")

            fig_scatter.update_layout(height=500)
            st.plotly_chart(fig_scatter, use_container_width=True)


def _render_detailed_analysis(umbral_similitud):
    """Análisis detallado por recomendación"""

    st.subheader("🔍 Análisis por Recomendación")

    # Obtener lista de recomendaciones
    top_recs = obtener_top_recomendaciones(umbral_similitud=umbral_similitud, limite=50)

    if top_recs.empty:
        st.warning("No hay recomendaciones disponibles")
        return

    # Selector de recomendación (sin key fijo)
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
            # Limpiar páginas de otras recomendaciones para evitar conflictos
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
            limite=100
        )

        if not municipios_impl.empty:
            # Filtro de búsqueda (key único por recomendación)
            search_term = st.text_input(
                "🔍 Buscar municipio:",
                placeholder="Escriba el nombre del municipio...",
                key=f"search_{selected_rec}"
            )

            # Aplicar filtro
            if search_term:
                mask = municipios_impl['Municipio'].str.contains(search_term, case=False, na=False)
                municipios_filtered = municipios_impl[mask]
                # Resetear página cuando se busca
                st.session_state[pagina_key] = 1
            else:
                municipios_filtered = municipios_impl

            # Paginación
            municipios_por_pagina = 10
            total_municipios = len(municipios_filtered)
            total_paginas = max(1, (total_municipios - 1) // municipios_por_pagina + 1)

            pagina_actual = st.session_state[pagina_key]

            # Validar página actual
            if pagina_actual > total_paginas:
                st.session_state[pagina_key] = 1
                pagina_actual = 1

            # Aplicar paginación
            inicio = (pagina_actual - 1) * municipios_por_pagina
            fin = inicio + municipios_por_pagina
            municipios_pagina = municipios_filtered.iloc[inicio:fin]

            # Mostrar info de paginación
            st.info(
                f"📊 Mostrando {len(municipios_pagina)} de {total_municipios} municipios (Página {pagina_actual} de {total_paginas})")

            # Mostrar municipios con tarjetas HTML
            st.markdown("**Municipios que implementan esta recomendación:**")

            for idx, (_, row) in enumerate(municipios_pagina.iterrows()):
                ranking_pos = inicio + idx + 1

                # Crear tarjeta estilizada
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