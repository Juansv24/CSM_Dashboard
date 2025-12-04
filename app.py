import streamlit as st
import time
import uuid
from datetime import datetime, timedelta
from vista_general import render_vista_general
from vista_departamental import render_ficha_departamental
from vista_municipal import render_ficha_municipal
from google_drive_client import (
    conectar_duckdb_parquet,
    obtener_metadatos_basicos
)
from logger_config import (
    logger,
    log_session_event,
    log_error_with_context,
    LoggingContext,
    log_streamlit_event
)

st.set_page_config(
    page_title="Dashboard de Similitudes Jerárquicas",
    page_icon="📊",
    layout="wide"
)


def _validar_sesion_activa(timeout_minutos: int = 30) -> bool:
    """
    Valida si la sesión sigue activa según tiempo de inactividad.

    Características:
    - Detecta inactividad del usuario (último tiempo registrado)
    - Limpia recursos (DuckDB connection) después de timeout
    - Retorna True si sesión está activa, False si expiró
    - Logs all session lifecycle events for monitoring

    Args:
        timeout_minutos: Minutos de inactividad antes de expirar sesión (default: 30)

    Returns:
        bool: True si sesión activa, False si expiró
    """
    ahora = datetime.now()
    session_id = st.session_state.get('session_id', 'unknown')

    # Inicializar timestamp de actividad si no existe
    if 'ultima_actividad' not in st.session_state:
        st.session_state.ultima_actividad = ahora
        log_session_event('SESSION_INITIALIZED', session_id)
        return True

    # Calcular tiempo desde última actividad
    tiempo_inactivo = ahora - st.session_state.ultima_actividad

    # Si pasó el timeout, limpiar recursos y retornar False
    if tiempo_inactivo > timedelta(minutes=timeout_minutos):
        log_session_event('SESSION_TIMEOUT', session_id,
                         details={'timeout_minutes': timeout_minutos,
                                 'inactive_minutes': tiempo_inactivo.total_seconds() / 60})

        # Cerrar conexión DuckDB si existe
        if 'duckdb_conn' in st.session_state and st.session_state.duckdb_conn:
            try:
                st.session_state.duckdb_conn.close()
                log_session_event('CONNECTION_CLOSED', session_id)
            except Exception as e:
                log_error_with_context(e, 'Connection cleanup on timeout', session_id=session_id)
            st.session_state.duckdb_conn = None

        # Limpiar timestamp de actividad para nueva sesión
        del st.session_state.ultima_actividad
        return False

    # Actualizar timestamp de actividad (el usuario sigue activo)
    st.session_state.ultima_actividad = ahora
    return True


def _inicializar_sesion_usuario() -> str:
    """
    Inicializa sesión única para cada usuario.

    Características:
    - Genera UUID único por sesión (no compartido entre usuarios)
    - Aísla datos de usuario para evitar contaminación cruzada
    - Registra timestamp de inicio de sesión
    - Permite tracking independiente de cada usuario

    Returns:
        str: ID único de sesión (UUID4)
    """
    # IMPROVEMENT #6: Generar ID único de sesión si no existe
    if 'session_id' not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())
        st.session_state.session_start = datetime.now()

    return st.session_state.session_id


def main():
    st.markdown("# 📊 Dashboard de Similitudes Jerárquicas")

    # IMPROVEMENT #5: Validar sesión activa y limpiar recursos si expiró
    if not _validar_sesion_activa(timeout_minutos=30):
        log_streamlit_event('SESSION_EXPIRED')
        st.warning("⏱️ Sesión expirada por inactividad (30 minutos). Recargando...")
        time.sleep(1)
        st.rerun()

    # IMPROVEMENT #6: Inicializar sesión única de usuario
    session_id = _inicializar_sesion_usuario()
    log_session_event('SESSION_ACTIVE', session_id)

    # Inicializar conexión a DuckDB
    if 'duckdb_conn' not in st.session_state:
        with LoggingContext('Database Connection', log_details={'session_id': session_id}):
            try:
                conn = conectar_duckdb_parquet()
                st.session_state.duckdb_conn = conn

                if conn is None:
                    log_error_with_context(
                        Exception("DuckDB connection returned None"),
                        "Database initialization",
                        session_id=session_id
                    )
                    st.error("❌ No se pudo conectar a la base de datos")
                    st.stop()

                log_session_event('DATABASE_CONNECTED', session_id)

            except Exception as e:
                log_error_with_context(e, "Database connection initialization", session_id=session_id)
                st.error(f"❌ Error conectando a la base de datos: {str(e)}")
                st.stop()

    # Cargar metadatos básicos
    with LoggingContext('Load Metadata', log_details={'session_id': session_id}):
        try:
            metadatos = obtener_metadatos_basicos()
            if not metadatos:
                log_error_with_context(
                    Exception("Metadata query returned empty"),
                    "Metadata loading",
                    session_id=session_id
                )
                st.error("❌ No se pudieron obtener los metadatos")
                st.stop()

            log_session_event('METADATA_LOADED', session_id,
                            details={'total_registros': metadatos.get('total_registros', 0)})

        except Exception as e:
            log_error_with_context(e, "Metadata loading", session_id=session_id)
            st.error(f"❌ Error cargando metadatos: {str(e)}")
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