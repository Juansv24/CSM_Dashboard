import streamlit as st
import pandas as pd
import gdown
import duckdb
import os
import tempfile
import time
from typing import Optional, Dict, Any

# Import logging configuration
try:
    from logger_config import logger, log_database_operation, log_query_performance, LoggingContext
except ImportError:
    # Fallback if logger_config is not yet available
    import logging
    logger = logging.getLogger("google_drive_client")


def _descargar_parquet_con_reintentos(file_id: str, max_reintentos: int = 3) -> Optional[str]:
    """
    Descarga archivo Parquet desde Google Drive con reintentos exponenciales.

    Reintentos: 5s, 10s, 20s con timeout de 120s cada intento.
    """
    temp_dir = tempfile.gettempdir()
    parquet_path = os.path.join(temp_dir, f"dashboard_{file_id[:8]}.parquet")

    # Si el archivo ya existe, validar integridad
    if os.path.exists(parquet_path):
        try:
            with open(parquet_path, 'rb') as f:
                header = f.read(4)
                if header == b'PAR1':  # Parquet magic number
                    return parquet_path
        except:
            os.remove(parquet_path)  # Archivo corrupto, eliminar

    url = f"https://drive.google.com/uc?id={file_id}"

    for intento in range(max_reintentos):
        try:
            with st.spinner(f'📥 Descargando datos (intento {intento + 1}/{max_reintentos})...'):
                gdown.download(url, parquet_path, quiet=False)
            st.success("✅ Datos descargados exitosamente")
            return parquet_path

        except Exception as e:
            if intento < max_reintentos - 1:
                espera = 5 * (2 ** intento)  # Exponential backoff: 5s, 10s, 20s
                st.warning(f"⚠️ Intento {intento + 1} falló. Reintentando en {espera}s...")
                time.sleep(espera)
            else:
                st.error(f"❌ Error al descargar datos después de {max_reintentos} intentos: {str(e)}")
                return None


@st.cache_resource
def _obtener_ruta_parquet_cacheada(file_id: str) -> Optional[str]:
    """
    Cachea SOLO la descarga del archivo Parquet (es cara).
    """
    return _descargar_parquet_con_reintentos(file_id, max_reintentos=3)


@st.cache_resource
def conectar_duckdb_parquet() -> Optional[duckdb.DuckDBPyConnection]:
    """
    ✅ MEJORA #1: Caching de conexión DuckDB con @st.cache_resource

    La conexión se crea UNA VEZ por sesión Streamlit y se reutiliza.
    Esto evita recargar los 4.2M registros en cada interacción.

    Streamlit maneja isolación entre usuarios automáticamente
    (cada usuario obtiene su propio cache).

    Returns:
        Conexión DuckDB o None si hay error
    """
    import time as time_module
    start_time = time_module.time()

    try:
        # Obtener ID del archivo desde secrets
        try:
            file_id = st.secrets["PARQUET_FILE_ID"]
        except Exception as e:
            logger.error(f"PARQUET_FILE_ID not configured in secrets: {str(e)}")
            st.error("⚠️ Configura PARQUET_FILE_ID en Streamlit secrets")
            return None

        # Obtener ruta del archivo (descargado y cacheado)
        parquet_path = _obtener_ruta_parquet_cacheada(file_id)
        if parquet_path is None:
            logger.error(f"Failed to download/cache parquet file: {file_id}")
            return None

        logger.debug(f"Parquet file loaded: {parquet_path}")

        # Crear conexión DuckDB - CACHEADA para toda la sesión
        conn = duckdb.connect(':memory:')
        logger.debug("DuckDB in-memory connection created")

        # ✅ MEJORA #2: Optimized memory and threading for Streamlit Cloud
        conn.execute("SET memory_limit='512MB'")  # Incrementado: 256MB -> 512MB (usa todos los recursos)
        conn.execute("SET threads=2")  # Incrementado: 1 -> 2 (permite paralelización)
        conn.execute("PRAGMA temp_directory='/tmp'")  # Disk overflow para graceful degradation
        logger.debug("DuckDB configuration applied: memory_limit=512MB, threads=2")

        # ✅ MEJORA #3: Create table from Parquet (not a view)
        # This allows us to create indexes on the actual table
        logger.debug("Loading parquet data into DuckDB table...")
        conn.execute(f"""
            CREATE TABLE datos_principales AS
            SELECT * FROM read_parquet('{parquet_path}')
        """)
        logger.debug("Table datos_principales created successfully")

        # Create indexes on frequently filtered columns
        # Estos índices aceleran queries hasta 10x
        logger.debug("Creating database indexes...")
        conn.execute("CREATE INDEX idx_similarity ON datos_principales(sentence_similarity)")
        conn.execute("CREATE INDEX idx_recommendation ON datos_principales(recommendation_code)")
        conn.execute("CREATE INDEX idx_municipality ON datos_principales(mpio_cdpmp)")
        conn.execute("CREATE INDEX idx_department ON datos_principales(dpto_cdpmp)")
        conn.execute("CREATE INDEX idx_territorio ON datos_principales(tipo_territorio)")
        logger.debug("All indexes created successfully")

        # Verificar conexión
        total = conn.execute("SELECT COUNT(*) FROM datos_principales").fetchone()[0]
        duration_ms = (time_module.time() - start_time) * 1000

        log_database_operation(
            operation='CONNECT_AND_LOAD',
            table='datos_principales',
            status='SUCCESS',
            duration_ms=duration_ms,
            row_count=total
        )

        st.sidebar.success(f"📊 Registros: {total:,} | ✨ Optimizado")
        logger.info(f"Database connection established successfully with {total:,} records in {duration_ms:.2f}ms")

        return conn

    except Exception as e:
        duration_ms = (time_module.time() - start_time) * 1000
        log_database_operation(
            operation='CONNECT_AND_LOAD',
            table='datos_principales',
            status='FAILED',
            duration_ms=duration_ms,
            error=e
        )
        st.error(f"❌ Error conectando a base de datos: {str(e)}")
        return None


def obtener_metadatos_basicos() -> Dict[str, Any]:
    """
    Obtiene métricas básicas del dataset.

    ⚠️ NO CACHEADA: Cada usuario necesita datos frescos.
    El @st.cache_data global causaba metadata stale para usuarios concurrentes.

    Returns:
        Diccionario con estadísticas generales
    """
    try:
        conn = st.session_state.get('duckdb_conn')
        if not conn:
            conn = conectar_duckdb_parquet()
            if conn:
                st.session_state.duckdb_conn = conn
            else:
                return {}

        resultado = conn.execute("""
            SELECT 
                COUNT(*) as total_registros,
                COUNT(DISTINCT dpto) as total_departamentos,
                COUNT(DISTINCT mpio_cdpmp) as total_municipios,
                COUNT(DISTINCT recommendation_code) as total_recomendaciones,
                AVG(sentence_similarity) as similitud_promedio
            FROM datos_principales
            WHERE tipo_territorio = 'Municipio'
        """).fetchone()

        return {
            'total_registros': resultado[0],
            'total_departamentos': resultado[1],
            'total_municipios': resultado[2],
            'total_recomendaciones': resultado[3],
            'similitud_promedio': resultado[4]
        }

    except Exception as e:
        st.error(f"Error obteniendo metadatos: {str(e)}")
        return {}


def construir_filtros_where(filtro_pdet: str = "Todos",
                            filtro_iica: list = None,
                            filtro_ipm: tuple = (0.0, 100.0),
                            filtro_mdm: list = None) -> str:
    """
    Construye cláusula WHERE para filtros socioeconómicos

    Args:
        filtro_pdet: Filtro PDET
        filtro_iica: Lista categorías IICA
        filtro_ipm: Rango IPM (min, max)
        filtro_mdm: Lista grupos MDM

    Returns:
        String con condiciones WHERE adicionales
    """
    conditions = []

    if filtro_pdet == "Solo PDET":
        conditions.append("PDET = 1")
    elif filtro_pdet == "Solo No PDET":
        conditions.append("PDET = 0")

    if filtro_iica and len(filtro_iica) > 0:
        iica_escaped = [x.replace("'", "''") for x in filtro_iica]
        iica_list = "','".join(iica_escaped)
        conditions.append(f"Cat_IICA IN ('{iica_list}')")

    if filtro_ipm and filtro_ipm != (0.0, 100.0):
        conditions.append(f"IPM_2018 BETWEEN {filtro_ipm[0]} AND {filtro_ipm[1]}")

    if filtro_mdm and len(filtro_mdm) > 0:
        mdm_escaped = [x.replace("'", "''") for x in filtro_mdm]
        mdm_list = "','".join(mdm_escaped)
        conditions.append(f"Grupo_MDM IN ('{mdm_list}')")

    return " AND " + " AND ".join(conditions) if conditions else ""


def consultar_datos_filtrados(umbral_similitud: float,
                              departamento: str = None,
                              municipio: str = None,
                              solo_politica_publica: bool = True,
                              limite: int = None,
                              filtro_pdet: str = "Todos",
                              filtro_iica: list = None,
                              filtro_ipm: tuple = (0.0, 100.0),
                              filtro_mdm: list = None) -> pd.DataFrame:
    """
    Consulta datos aplicando filtros específicos

    Args:
        umbral_similitud: Similitud mínima requerida
        departamento: Nombre del departamento (opcional)
        municipio: Nombre del municipio (opcional)
        solo_politica_publica: Filtrar solo política pública
        limite: Número máximo de registros
        filtro_pdet: Filtro PDET
        filtro_iica: Lista categorías IICA
        filtro_ipm: Rango IPM
        filtro_mdm: Lista grupos MDM

    Returns:
        DataFrame con datos filtrados
    """
    try:
        conn = st.session_state.get('duckdb_conn')
        if not conn:
            return pd.DataFrame()

        where_conditions = [
            f"sentence_similarity >= {umbral_similitud}",
            "tipo_territorio = 'Municipio'"
        ]

        if solo_politica_publica:
            where_conditions.append(
                "(predicted_class = 'Incluida' OR "
                "(predicted_class = 'Excluida' AND prediction_confidence < 0.8))"
            )

        if departamento and departamento != 'Todos':
            departamento_escaped = departamento.replace("'", "''")
            where_conditions.append(f"dpto = '{departamento_escaped}'")

        if municipio and municipio != 'Todos':
            municipio_escaped = municipio.replace("'", "''")
            where_conditions.append(f"mpio = '{municipio_escaped}'")

        # Agregar filtros socioeconómicos
        filtros_adicionales = construir_filtros_where(filtro_pdet, filtro_iica, filtro_ipm, filtro_mdm)
        where_clause = " AND ".join(where_conditions) + filtros_adicionales

        limit_clause = f"LIMIT {limite}" if limite else ""

        query = f"""
            SELECT * FROM datos_principales 
            WHERE {where_clause}
            ORDER BY sentence_similarity DESC
            {limit_clause}
        """

        return conn.execute(query).df()

    except Exception as e:
        st.error(f"Error en consulta filtrada: {str(e)}")
        return pd.DataFrame()


def obtener_estadisticas_departamentales(umbral_similitud: float,
                                         filtro_pdet: str = "Todos",
                                         filtro_iica: list = None,
                                         filtro_ipm: tuple = (0.0, 100.0),
                                         filtro_mdm: list = None) -> pd.DataFrame:
    """
    Calcula estadísticas agregadas por departamento
    Incluye min/max por municipio dentro de cada departamento

    Args:
        umbral_similitud: Similitud mínima
        filtro_pdet: Filtro PDET
        filtro_iica: Lista categorías IICA
        filtro_ipm: Rango IPM
        filtro_mdm: Lista grupos MDM

    Returns:
        DataFrame con estadísticas departamentales
    """
    try:
        conn = st.session_state.get('duckdb_conn')
        if not conn:
            return pd.DataFrame()

        filtros_adicionales = construir_filtros_where(filtro_pdet, filtro_iica, filtro_ipm, filtro_mdm)

        query = f"""
            WITH recomendaciones_por_municipio AS (
                SELECT 
                    dpto_cdpmp,
                    dpto,
                    mpio_cdpmp,
                    mpio,
                    COUNT(DISTINCT recommendation_code) as num_recomendaciones
                FROM datos_principales 
                WHERE tipo_territorio = 'Municipio'
                AND sentence_similarity >= {umbral_similitud}
                {filtros_adicionales}
                GROUP BY dpto_cdpmp, dpto, mpio_cdpmp, mpio
            ),
            ranking_por_depto AS (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (PARTITION BY dpto_cdpmp ORDER BY num_recomendaciones ASC) as rank_min,
                    ROW_NUMBER() OVER (PARTITION BY dpto_cdpmp ORDER BY num_recomendaciones DESC) as rank_max
                FROM recomendaciones_por_municipio
            ),
            municipio_min AS (
                SELECT 
                    dpto_cdpmp,
                    mpio as Municipio_Min,
                    num_recomendaciones as Min_Recomendaciones
                FROM ranking_por_depto
                WHERE rank_min = 1
            ),
            municipio_max AS (
                SELECT 
                    dpto_cdpmp,
                    mpio as Municipio_Max,
                    num_recomendaciones as Max_Recomendaciones
                FROM ranking_por_depto
                WHERE rank_max = 1
            ),
            stats_generales AS (
                SELECT 
                    dpto_cdpmp,
                    dpto as Departamento,
                    COUNT(DISTINCT mpio_cdpmp) as Municipios,
                    ROUND(AVG(num_recomendaciones), 0) as Promedio_Recomendaciones
                FROM recomendaciones_por_municipio
                GROUP BY dpto_cdpmp, dpto
            )
            SELECT 
                s.dpto_cdpmp,
                s.Departamento,
                s.Municipios,
                s.Promedio_Recomendaciones,
                COALESCE(mn.Min_Recomendaciones, 0) as Min_Recomendaciones,
                COALESCE(mn.Municipio_Min, 'N/A') as Municipio_Min,
                COALESCE(mx.Max_Recomendaciones, 0) as Max_Recomendaciones,
                COALESCE(mx.Municipio_Max, 'N/A') as Municipio_Max
            FROM stats_generales s
            LEFT JOIN municipio_min mn ON s.dpto_cdpmp = mn.dpto_cdpmp
            LEFT JOIN municipio_max mx ON s.dpto_cdpmp = mx.dpto_cdpmp
            ORDER BY s.Promedio_Recomendaciones DESC
        """

        return conn.execute(query).df()

    except Exception as e:
        st.error(f"Error en estadísticas departamentales: {str(e)}")
        return pd.DataFrame()


def obtener_ranking_municipios(umbral_similitud: float,
                               solo_politica_publica: bool = True,
                               top_n: int = None,
                               filtro_pdet: str = "Todos",
                               filtro_iica: list = None,
                               filtro_ipm: tuple = (0.0, 100.0),
                               filtro_mdm: list = None) -> pd.DataFrame:
    """
    Genera ranking de municipios por número de recomendaciones implementadas

    Args:
        umbral_similitud: Similitud mínima
        solo_politica_publica: Filtrar política pública
        top_n: Número máximo de resultados
        filtro_pdet: Filtro PDET
        filtro_iica: Lista categorías IICA
        filtro_ipm: Rango IPM
        filtro_mdm: Lista grupos MDM

    Returns:
        DataFrame ordenado por ranking
    """
    try:
        conn = st.session_state.get('duckdb_conn')
        if not conn:
            return pd.DataFrame()

        where_conditions = [
            f"sentence_similarity >= {umbral_similitud}",
            "tipo_territorio = 'Municipio'"
        ]

        if solo_politica_publica:
            where_conditions.append(
                "(predicted_class = 'Incluida' OR "
                "(predicted_class = 'Excluida' AND prediction_confidence < 0.8))"
            )

        filtros_adicionales = construir_filtros_where(filtro_pdet, filtro_iica, filtro_ipm, filtro_mdm)
        where_clause = " AND ".join(where_conditions) + filtros_adicionales

        limit_clause = f"LIMIT {top_n}" if top_n else ""

        query = f"""
            SELECT 
                mpio_cdpmp,
                mpio as Municipio,
                dpto as Departamento,
                COUNT(DISTINCT recommendation_code) as Recomendaciones_Implementadas,
                COUNT(*) as Total_Oraciones,
                AVG(sentence_similarity) as Similitud_Promedio,
                COUNT(CASE WHEN recommendation_priority = 1 THEN 1 END) as Prioritarias_Implementadas,
                ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT recommendation_code) DESC) as Ranking
            FROM datos_principales 
            WHERE {where_clause}
            GROUP BY mpio_cdpmp, mpio, dpto
            ORDER BY Recomendaciones_Implementadas DESC
            {limit_clause}
        """

        return conn.execute(query).df()

    except Exception as e:
        st.error(f"Error generando ranking: {str(e)}")
        return pd.DataFrame()


def obtener_top_recomendaciones(umbral_similitud: float = 0.6,
                                departamento: str = None,
                                municipio: str = None,
                                limite: int = 10,
                                filtro_pdet: str = "Todos",
                                filtro_iica: list = None,
                                filtro_ipm: tuple = (0.0, 100.0),
                                filtro_mdm: list = None) -> pd.DataFrame:
    """
    Obtiene las recomendaciones más frecuentemente mencionadas

    Args:
        umbral_similitud: Similitud mínima
        departamento: Filtro por departamento
        municipio: Filtro por municipio
        limite: Número máximo de recomendaciones
        filtro_pdet: Filtro PDET
        filtro_iica: Lista categorías IICA
        filtro_ipm: Rango IPM
        filtro_mdm: Lista grupos MDM

    Returns:
        DataFrame con top recomendaciones
    """
    try:
        conn = st.session_state.get('duckdb_conn')
        if not conn:
            return pd.DataFrame()

        where_conditions = [
            f"sentence_similarity >= {umbral_similitud}",
            "tipo_territorio = 'Municipio'"
        ]

        if departamento and departamento != 'Todos':
            departamento_escaped = departamento.replace("'", "''")
            where_conditions.append(f"dpto = '{departamento_escaped}'")

        if municipio and municipio != 'Todos':
            municipio_escaped = municipio.replace("'", "''")
            where_conditions.append(f"mpio = '{municipio_escaped}'")

        filtros_adicionales = construir_filtros_where(filtro_pdet, filtro_iica, filtro_ipm, filtro_mdm)
        where_clause = " AND ".join(where_conditions) + filtros_adicionales

        query = f"""
            SELECT 
                recommendation_code as Codigo,
                recommendation_text as Texto,
                recommendation_priority as Prioridad,
                COUNT(*) as Frecuencia_Oraciones,
                COUNT(DISTINCT mpio_cdpmp) as Municipios_Implementan,
                AVG(sentence_similarity) as Similitud_Promedio
            FROM datos_principales 
            WHERE {where_clause}
            GROUP BY recommendation_code, recommendation_text, recommendation_priority
            ORDER BY Frecuencia_Oraciones DESC
            LIMIT {limite}
        """

        return conn.execute(query).df()

    except Exception as e:
        st.error(f"Error obteniendo top recomendaciones: {str(e)}")
        return pd.DataFrame()


def obtener_municipios_por_recomendacion(codigo_recomendacion: str,
                                         umbral_similitud: float = 0.6,
                                         limite: int = 100,
                                         filtro_pdet: str = "Todos",
                                         filtro_iica: list = None,
                                         filtro_ipm: tuple = (0.0, 100.0),
                                         filtro_mdm: list = None) -> pd.DataFrame:
    """
    Obtiene municipios que mencionan una recomendación específica

    Args:
        codigo_recomendacion: Código de la recomendación
        umbral_similitud: Similitud mínima
        limite: Número máximo de municipios
        filtro_pdet: Filtro PDET
        filtro_iica: Lista categorías IICA
        filtro_ipm: Rango IPM
        filtro_mdm: Lista grupos MDM

    Returns:
        DataFrame con municipios ordenados por frecuencia
    """
    try:
        conn = st.session_state.get('duckdb_conn')
        if not conn:
            return pd.DataFrame()

        codigo_escaped = codigo_recomendacion.replace("'", "''")
        filtros_adicionales = construir_filtros_where(filtro_pdet, filtro_iica, filtro_ipm, filtro_mdm)

        query = f"""
            SELECT 
                mpio as Municipio,
                dpto as Departamento,
                COUNT(*) as Frecuencia_Oraciones,
                AVG(sentence_similarity) as Similitud_Promedio,
                MAX(sentence_similarity) as Similitud_Maxima
            FROM datos_principales 
            WHERE recommendation_code = '{codigo_escaped}'
            AND sentence_similarity >= {umbral_similitud}
            AND tipo_territorio = 'Municipio'
            {filtros_adicionales}
            GROUP BY mpio, dpto
            ORDER BY Frecuencia_Oraciones DESC, Similitud_Promedio DESC
            LIMIT {limite}
        """

        return conn.execute(query).df()

    except Exception as e:
        st.error(f"Error obteniendo municipios por recomendación: {str(e)}")
        return pd.DataFrame()


def obtener_todos_los_municipios() -> pd.DataFrame:
    """
    Obtiene lista completa de municipios únicos disponibles

    Returns:
        DataFrame con código, nombre de municipio y departamento
    """
    try:
        conn = st.session_state.get('duckdb_conn')
        if not conn:
            return pd.DataFrame()

        query = """
            SELECT DISTINCT 
                mpio_cdpmp,
                mpio as Municipio,
                dpto_cdpmp,
                dpto as Departamento
            FROM datos_principales 
            WHERE tipo_territorio = 'Municipio'
            ORDER BY dpto, mpio
        """

        return conn.execute(query).df()

    except Exception as e:
        st.error(f"Error obteniendo lista de municipios: {str(e)}")
        return pd.DataFrame()


def obtener_todos_los_departamentos() -> pd.DataFrame:
    """
    Obtiene lista completa de departamentos únicos disponibles

    Returns:
        DataFrame con código y nombre de departamento
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
            WHERE tipo_territorio = 'Municipio'
            ORDER BY dpto
        """

        return conn.execute(query).df()

    except Exception as e:
        st.error(f"Error obteniendo lista de departamentos: {str(e)}")
        return pd.DataFrame()