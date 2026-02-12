"""
Data client for DuckDB with persistent in-memory database.
Optimized for Hugging Face Spaces deployment.
Thread-safe: parquet loaded once into memory, shared connection with lock.
"""
import streamlit as st
import pandas as pd
import duckdb
import os
import threading
from typing import Optional, Dict, Any, List

# Path to local parquet file
PARQUET_PATH = "Data/Data Final Dashboard.parquet"
DATA_TABLE = "data"

# Default columns used by most views (excludes heavy text fields)
DEFAULT_COLUMNS = [
    'mpio_cdpmp', 'mpio', 'dpto_cdpmp', 'dpto',
    'tipo_territorio',
    'recommendation_code', 'recommendation_text',
    'recommendation_topic', 'recommendation_priority',
    'sentence_text', 'sentence_similarity',
    'paragraph_text', 'paragraph_similarity',
    'paragraph_id', 'page_number',
    'sentence_id_paragraph',
    'predicted_class', 'prediction_confidence',
    'IPM_2018', 'PDET', 'Cat_IICA', 'Grupo_MDM'
]


@st.cache_resource
def _init_db():
    """
    One-time load of parquet into an in-memory DuckDB table.
    Cached for the lifetime of the app process. No extra files on disk.
    Returns (connection, lock) for thread-safe access.
    """
    if not os.path.exists(PARQUET_PATH):
        raise FileNotFoundError(f"Archivo de datos no encontrado: {PARQUET_PATH}")

    conn = duckdb.connect(':memory:')
    conn.execute(f"""
        CREATE TABLE {DATA_TABLE} AS
        SELECT * FROM read_parquet('{PARQUET_PATH}')
    """)
    lock = threading.Lock()
    return conn, lock


def _execute_query(query: str) -> list:
    """
    Execute a query on the shared in-memory database.
    Thread-safe via lock. Returns raw results as a list of tuples.
    """
    conn, lock = _init_db()
    with lock:
        return conn.execute(query).fetchall()


def _execute_query_df(query: str) -> pd.DataFrame:
    """
    Execute a query on the shared in-memory database.
    Thread-safe via lock. Returns results as a DataFrame.
    """
    conn, lock = _init_db()
    with lock:
        return conn.execute(query).df()


def construir_filtros_where(filtro_pdet: str = "Todos",
                            filtro_iica: list = None,
                            filtro_ipm: tuple = (0.0, 100.0),
                            filtro_mdm: list = None) -> str:
    """
    Construye cláusula WHERE para filtros socioeconómicos.

    Args:
        filtro_pdet: Filtro PDET ("Todos", "Solo PDET", "Solo No PDET")
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


@st.cache_data
def obtener_metadatos_basicos() -> Dict[str, Any]:
    """
    Obtiene métricas básicas del dataset.
    Cached permanently: underlying data does not change.

    Returns:
        Diccionario con estadísticas generales
    """
    try:
        resultado = _execute_query(f"""
            SELECT
                COUNT(*) as total_registros,
                COUNT(DISTINCT dpto) as total_departamentos,
                COUNT(DISTINCT mpio_cdpmp) as total_municipios,
                COUNT(DISTINCT recommendation_code) as total_recomendaciones,
                AVG(sentence_similarity) as similitud_promedio
            FROM {DATA_TABLE}
            WHERE tipo_territorio = 'Municipio'
        """)

        if resultado:
            row = resultado[0]
            return {
                'total_registros': row[0],
                'total_departamentos': row[1],
                'total_municipios': row[2],
                'total_recomendaciones': row[3],
                'similitud_promedio': row[4]
            }
        return {}

    except Exception as e:
        st.error(f"Error obteniendo metadatos: {str(e)}")
        return {}


@st.cache_data(ttl=300)
def obtener_metadatos_filtrados(umbral_similitud: float,
                                filtro_pdet: str = "Todos",
                                filtro_iica: tuple = (),
                                filtro_mdm: tuple = ()) -> Dict[str, Any]:
    """
    Obtiene métricas básicas aplicando filtros.

    Args:
        umbral_similitud: Similitud mínima
        filtro_pdet: Filtro PDET
        filtro_iica: Tuple de categorías IICA (hashable)
        filtro_mdm: Tuple de grupos MDM (hashable)

    Returns:
        Diccionario con estadísticas filtradas
    """
    try:
        filtros_adicionales = construir_filtros_where(
            filtro_pdet, list(filtro_iica) or None, (0.0, 100.0), list(filtro_mdm) or None
        )

        resultado = _execute_query(f"""
            SELECT
                COUNT(*) as total_registros,
                COUNT(DISTINCT dpto) as total_departamentos,
                COUNT(DISTINCT mpio_cdpmp) as total_municipios,
                COUNT(DISTINCT recommendation_code) as total_recomendaciones,
                AVG(sentence_similarity) as similitud_promedio
            FROM {DATA_TABLE}
            WHERE sentence_similarity >= {umbral_similitud}
            AND tipo_territorio = 'Municipio'
            {filtros_adicionales}
        """)

        if resultado:
            row = resultado[0]
            return {
                'total_registros': row[0],
                'total_departamentos': row[1],
                'total_municipios': row[2],
                'total_recomendaciones': row[3],
                'similitud_promedio': row[4]
            }
        return {}

    except Exception as e:
        st.error(f"Error obteniendo metadatos filtrados: {str(e)}")
        return {}


@st.cache_data(ttl=300)
def consultar_datos_filtrados(umbral_similitud: float,
                              departamento: str = None,
                              municipio: str = None,
                              solo_politica_publica: bool = True,
                              limite: int = None,
                              filtro_pdet: str = "Todos",
                              filtro_iica: tuple = (),
                              filtro_ipm: tuple = (0.0, 100.0),
                              filtro_mdm: tuple = (),
                              tipo_territorio: str = 'Municipio',
                              columns: tuple = None) -> pd.DataFrame:
    """
    Consulta datos aplicando filtros específicos.

    Args:
        umbral_similitud: Similitud mínima requerida
        departamento: Nombre del departamento (opcional)
        municipio: Nombre del municipio (opcional)
        solo_politica_publica: Filtrar solo política pública
        limite: Número máximo de registros
        filtro_pdet: Filtro PDET
        filtro_iica: Tuple de categorías IICA (hashable)
        filtro_ipm: Rango IPM (min, max)
        filtro_mdm: Tuple de grupos MDM (hashable)
        tipo_territorio: 'Municipio' o 'Departamento'
        columns: Tuple of column names to select (None = DEFAULT_COLUMNS)

    Returns:
        DataFrame con datos filtrados
    """
    try:
        cols = list(columns) if columns else DEFAULT_COLUMNS
        col_list = ", ".join(cols)

        where_conditions = [
            f"sentence_similarity >= {umbral_similitud}",
            f"tipo_territorio = '{tipo_territorio}'"
        ]

        if solo_politica_publica:
            where_conditions.append("predicted_class = 'Incluida'")
        else:
            where_conditions.append("predicted_class = 'Excluida'")

        if departamento and departamento != 'Todos':
            departamento_escaped = departamento.replace("'", "''")
            where_conditions.append(f"dpto = '{departamento_escaped}'")

        if municipio and municipio != 'Todos':
            municipio_escaped = municipio.replace("'", "''")
            where_conditions.append(f"mpio = '{municipio_escaped}'")

        # Add socioeconomic filters only for municipalities
        filtro_iica_list = list(filtro_iica) if filtro_iica else None
        filtro_ipm_tuple = tuple(filtro_ipm) if filtro_ipm else (0.0, 100.0)
        filtro_mdm_list = list(filtro_mdm) if filtro_mdm else None

        if tipo_territorio == 'Municipio':
            filtros_adicionales = construir_filtros_where(
                filtro_pdet, filtro_iica_list, filtro_ipm_tuple, filtro_mdm_list
            )
        else:
            filtros_adicionales = ""

        where_clause = " AND ".join(where_conditions) + filtros_adicionales
        limit_clause = f"LIMIT {limite}" if limite else ""

        query = f"""
            SELECT {col_list} FROM {DATA_TABLE}
            WHERE {where_clause}
            ORDER BY sentence_similarity DESC
            {limit_clause}
        """

        return _execute_query_df(query)

    except Exception as e:
        st.error(f"Error en consulta filtrada: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def obtener_estadisticas_departamentales(umbral_similitud: float,
                                         filtro_pdet: str = "Todos",
                                         filtro_iica: tuple = (),
                                         filtro_ipm: tuple = (0.0, 100.0),
                                         filtro_mdm: tuple = ()) -> pd.DataFrame:
    """
    Calcula estadísticas agregadas por departamento.
    Incluye min/max por municipio dentro de cada departamento.

    Returns:
        DataFrame con estadísticas departamentales
    """
    try:
        filtro_iica_list = list(filtro_iica) if filtro_iica else None
        filtro_ipm_tuple = tuple(filtro_ipm) if filtro_ipm else (0.0, 100.0)
        filtro_mdm_list = list(filtro_mdm) if filtro_mdm else None

        filtros_adicionales = construir_filtros_where(
            filtro_pdet, filtro_iica_list, filtro_ipm_tuple, filtro_mdm_list
        )

        query = f"""
            WITH recomendaciones_por_municipio AS (
                SELECT
                    dpto_cdpmp,
                    dpto,
                    mpio_cdpmp,
                    mpio,
                    COUNT(DISTINCT recommendation_code) as num_recomendaciones
                FROM {DATA_TABLE}
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

        return _execute_query_df(query)

    except Exception as e:
        st.error(f"Error en estadísticas departamentales: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def obtener_ranking_municipios(umbral_similitud: float,
                               solo_politica_publica: bool = True,
                               top_n: int = None,
                               filtro_pdet: str = "Todos",
                               filtro_iica: tuple = (),
                               filtro_ipm: tuple = (0.0, 100.0),
                               filtro_mdm: tuple = ()) -> pd.DataFrame:
    """
    Genera ranking de municipios por número de recomendaciones implementadas.

    Returns:
        DataFrame ordenado por ranking
    """
    try:
        where_conditions = [
            f"sentence_similarity >= {umbral_similitud}",
            "tipo_territorio = 'Municipio'"
        ]

        if solo_politica_publica:
            where_conditions.append("predicted_class = 'Incluida'")
        else:
            where_conditions.append("predicted_class = 'Excluida'")

        filtro_iica_list = list(filtro_iica) if filtro_iica else None
        filtro_ipm_tuple = tuple(filtro_ipm) if filtro_ipm else (0.0, 100.0)
        filtro_mdm_list = list(filtro_mdm) if filtro_mdm else None

        filtros_adicionales = construir_filtros_where(
            filtro_pdet, filtro_iica_list, filtro_ipm_tuple, filtro_mdm_list
        )
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
            FROM {DATA_TABLE}
            WHERE {where_clause}
            GROUP BY mpio_cdpmp, mpio, dpto
            ORDER BY Recomendaciones_Implementadas DESC
            {limit_clause}
        """

        return _execute_query_df(query)

    except Exception as e:
        st.error(f"Error generando ranking: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def obtener_ranking_municipio_especifico(municipio: str,
                                          umbral_similitud: float,
                                          solo_politica_publica: bool = True,
                                          filtro_pdet: str = "Todos",
                                          filtro_iica: tuple = (),
                                          filtro_ipm: tuple = (0.0, 100.0),
                                          filtro_mdm: tuple = ()) -> Dict[str, Any]:
    """
    Computes the rank for a specific municipality without loading all rankings.

    Returns:
        Dict with 'ranking_position' and 'total_municipios'
    """
    try:
        where_conditions = [
            f"sentence_similarity >= {umbral_similitud}",
            "tipo_territorio = 'Municipio'"
        ]

        if solo_politica_publica:
            where_conditions.append("predicted_class = 'Incluida'")
        else:
            where_conditions.append("predicted_class = 'Excluida'")

        filtro_iica_list = list(filtro_iica) if filtro_iica else None
        filtro_ipm_tuple = tuple(filtro_ipm) if filtro_ipm else (0.0, 100.0)
        filtro_mdm_list = list(filtro_mdm) if filtro_mdm else None

        filtros_adicionales = construir_filtros_where(
            filtro_pdet, filtro_iica_list, filtro_ipm_tuple, filtro_mdm_list
        )
        where_clause = " AND ".join(where_conditions) + filtros_adicionales

        municipio_escaped = municipio.replace("'", "''")

        query = f"""
            WITH ranking AS (
                SELECT
                    mpio,
                    COUNT(DISTINCT recommendation_code) as num_recs,
                    ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT recommendation_code) DESC) as rank_pos
                FROM {DATA_TABLE}
                WHERE {where_clause}
                GROUP BY mpio
            )
            SELECT
                (SELECT rank_pos FROM ranking WHERE mpio = '{municipio_escaped}') as ranking_position,
                COUNT(*) as total_municipios
            FROM ranking
        """

        resultado = _execute_query(query)
        if resultado:
            row = resultado[0]
            return {
                'ranking_position': row[0] if row[0] is not None else "N/A",
                'total_municipios': row[1]
            }
        return {'ranking_position': "N/A", 'total_municipios': 0}

    except Exception as e:
        st.error(f"Error obteniendo ranking específico: {str(e)}")
        return {'ranking_position': "N/A", 'total_municipios': 0}


@st.cache_data(ttl=300)
def obtener_ranking_departamento_especifico(departamento: str,
                                             umbral_similitud: float,
                                             solo_politica_publica: bool = True) -> Dict[str, Any]:
    """
    Computes the rank for a specific department without loading all rankings.

    Returns:
        Dict with 'ranking_position' and 'total_departamentos'
    """
    try:
        where_conditions = [
            f"sentence_similarity >= {umbral_similitud}",
            "tipo_territorio = 'Departamento'"
        ]

        if solo_politica_publica:
            where_conditions.append("predicted_class = 'Incluida'")
        else:
            where_conditions.append("predicted_class = 'Excluida'")

        where_clause = " AND ".join(where_conditions)
        departamento_escaped = departamento.replace("'", "''")

        query = f"""
            WITH ranking AS (
                SELECT
                    dpto,
                    COUNT(DISTINCT recommendation_code) as num_recs,
                    ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT recommendation_code) DESC) as rank_pos
                FROM {DATA_TABLE}
                WHERE {where_clause}
                GROUP BY dpto
            )
            SELECT
                (SELECT rank_pos FROM ranking WHERE dpto = '{departamento_escaped}') as ranking_position,
                COUNT(*) as total_departamentos
            FROM ranking
        """

        resultado = _execute_query(query)
        if resultado:
            row = resultado[0]
            return {
                'ranking_position': row[0] if row[0] is not None else "N/A",
                'total_departamentos': row[1]
            }
        return {'ranking_position': "N/A", 'total_departamentos': 0}

    except Exception as e:
        st.error(f"Error obteniendo ranking departamental: {str(e)}")
        return {'ranking_position': "N/A", 'total_departamentos': 0}


@st.cache_data(ttl=300)
def obtener_top_recomendaciones(umbral_similitud: float = 0.6,
                                departamento: str = None,
                                municipio: str = None,
                                limite: int = 10,
                                filtro_pdet: str = "Todos",
                                filtro_iica: tuple = (),
                                filtro_ipm: tuple = (0.0, 100.0),
                                filtro_mdm: tuple = ()) -> pd.DataFrame:
    """
    Obtiene las recomendaciones más frecuentemente mencionadas.

    Returns:
        DataFrame con top recomendaciones
    """
    try:
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

        filtro_iica_list = list(filtro_iica) if filtro_iica else None
        filtro_ipm_tuple = tuple(filtro_ipm) if filtro_ipm else (0.0, 100.0)
        filtro_mdm_list = list(filtro_mdm) if filtro_mdm else None

        filtros_adicionales = construir_filtros_where(
            filtro_pdet, filtro_iica_list, filtro_ipm_tuple, filtro_mdm_list
        )
        where_clause = " AND ".join(where_conditions) + filtros_adicionales

        query = f"""
            SELECT
                recommendation_code as Codigo,
                recommendation_text as Texto,
                recommendation_priority as Prioridad,
                COUNT(*) as Frecuencia_Oraciones,
                COUNT(DISTINCT mpio_cdpmp) as Municipios_Implementan,
                AVG(sentence_similarity) as Similitud_Promedio
            FROM {DATA_TABLE}
            WHERE {where_clause}
            GROUP BY recommendation_code, recommendation_text, recommendation_priority
            ORDER BY Frecuencia_Oraciones DESC
            LIMIT {limite}
        """

        return _execute_query_df(query)

    except Exception as e:
        st.error(f"Error obteniendo top recomendaciones: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def obtener_municipios_por_recomendacion(codigo_recomendacion: str,
                                         umbral_similitud: float = 0.6,
                                         limite: int = 100,
                                         filtro_pdet: str = "Todos",
                                         filtro_iica: tuple = (),
                                         filtro_ipm: tuple = (0.0, 100.0),
                                         filtro_mdm: tuple = ()) -> pd.DataFrame:
    """
    Obtiene municipios que mencionan una recomendación específica.

    Returns:
        DataFrame con municipios ordenados por frecuencia
    """
    try:
        codigo_escaped = codigo_recomendacion.replace("'", "''")
        filtro_iica_list = list(filtro_iica) if filtro_iica else None
        filtro_ipm_tuple = tuple(filtro_ipm) if filtro_ipm else (0.0, 100.0)
        filtro_mdm_list = list(filtro_mdm) if filtro_mdm else None

        filtros_adicionales = construir_filtros_where(
            filtro_pdet, filtro_iica_list, filtro_ipm_tuple, filtro_mdm_list
        )

        query = f"""
            SELECT
                mpio as Municipio,
                dpto as Departamento,
                COUNT(*) as Frecuencia_Oraciones,
                AVG(sentence_similarity) as Similitud_Promedio,
                MAX(sentence_similarity) as Similitud_Maxima
            FROM {DATA_TABLE}
            WHERE recommendation_code = '{codigo_escaped}'
            AND sentence_similarity >= {umbral_similitud}
            AND tipo_territorio = 'Municipio'
            {filtros_adicionales}
            GROUP BY mpio, dpto
            ORDER BY Frecuencia_Oraciones DESC, Similitud_Promedio DESC
            LIMIT {limite}
        """

        return _execute_query_df(query)

    except Exception as e:
        st.error(f"Error obteniendo municipios por recomendación: {str(e)}")
        return pd.DataFrame()


@st.cache_data
def obtener_todos_los_municipios() -> pd.DataFrame:
    """
    Obtiene lista completa de municipios únicos disponibles.
    Cached permanently: underlying data does not change.

    Returns:
        DataFrame con código, nombre de municipio y departamento
    """
    try:
        query = f"""
            SELECT DISTINCT
                mpio_cdpmp,
                mpio as Municipio,
                dpto_cdpmp,
                dpto as Departamento
            FROM {DATA_TABLE}
            WHERE tipo_territorio = 'Municipio'
            ORDER BY dpto, mpio
        """

        return _execute_query_df(query)

    except Exception as e:
        st.error(f"Error obteniendo lista de municipios: {str(e)}")
        return pd.DataFrame()


@st.cache_data
def obtener_todos_los_departamentos() -> pd.DataFrame:
    """
    Obtiene lista completa de departamentos únicos disponibles.
    Cached permanently: underlying data does not change.

    Returns:
        DataFrame con código y nombre de departamento
    """
    try:
        query = f"""
            SELECT DISTINCT
                dpto_cdpmp,
                dpto as Departamento
            FROM {DATA_TABLE}
            WHERE tipo_territorio = 'Municipio'
            ORDER BY dpto
        """

        return _execute_query_df(query)

    except Exception as e:
        st.error(f"Error obteniendo lista de departamentos: {str(e)}")
        return pd.DataFrame()


@st.cache_data
def obtener_todos_los_departamentos_territorio() -> pd.DataFrame:
    """
    Obtiene lista de departamentos únicos con datos tipo_territorio = 'Departamento'
    Cached permanently: underlying data does not change.

    Returns:
        DataFrame con departamentos disponibles
    """
    try:
        query = f"""
            SELECT DISTINCT
                dpto_cdpmp,
                dpto as Departamento
            FROM {DATA_TABLE}
            WHERE tipo_territorio = 'Departamento'
            ORDER BY dpto
        """

        return _execute_query_df(query)

    except Exception as e:
        st.error(f"Error obteniendo departamentos: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def obtener_ranking_departamentos(umbral_similitud: float,
                                  solo_politica_publica: bool = True,
                                  top_n: int = None) -> pd.DataFrame:
    """
    Genera ranking de departamentos por número de recomendaciones implementadas.

    Returns:
        DataFrame ordenado por ranking
    """
    try:
        where_conditions = [
            f"sentence_similarity >= {umbral_similitud}",
            "tipo_territorio = 'Departamento'"
        ]

        if solo_politica_publica:
            where_conditions.append("predicted_class = 'Incluida'")
        else:
            where_conditions.append("predicted_class = 'Excluida'")

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
            FROM {DATA_TABLE}
            WHERE {where_clause}
            GROUP BY dpto_cdpmp, dpto
            ORDER BY Recomendaciones_Implementadas DESC
            {limit_clause}
        """

        return _execute_query_df(query)

    except Exception as e:
        st.error(f"Error generando ranking departamental: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def obtener_datos_mapa_municipal(dpto_code: str, min_similarity: float) -> pd.DataFrame:
    """
    Obtiene datos agregados de municipios de un departamento para el mapa.

    Args:
        dpto_code: Código del departamento (se normaliza a 2 dígitos)
        min_similarity: Umbral de similitud mínima

    Returns:
        DataFrame con datos municipales agregados
    """
    try:
        dpto_code_normalized = str(dpto_code).zfill(2)

        query = f"""
            SELECT
                mpio_cdpmp,
                mpio as Municipio,
                dpto as Departamento,
                COUNT(DISTINCT recommendation_code) as Num_Recomendaciones,
                AVG(sentence_similarity) as Similitud_Promedio
            FROM {DATA_TABLE}
            WHERE dpto_cdpmp = '{dpto_code_normalized}'
            AND sentence_similarity >= {min_similarity}
            AND tipo_territorio = 'Municipio'
            GROUP BY mpio_cdpmp, mpio, dpto
            ORDER BY Num_Recomendaciones DESC
        """

        return _execute_query_df(query)

    except Exception as e:
        st.error(f"Error obteniendo datos de mapa municipal: {str(e)}")
        return pd.DataFrame()
