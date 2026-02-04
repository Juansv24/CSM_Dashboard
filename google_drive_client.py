"""
PyArrow-based Query Layer for CSM Dashboard (Render.com Deployment)

This module provides memory-efficient data access using PyArrow predicate pushdown
instead of loading the entire dataset into DuckDB. This approach allows the dashboard
to run within Render's 512MB RAM constraint.

Architecture:
- Startup: Cache ParquetFile metadata only (~5MB)
- Per query: Use PyArrow filters to load only matching rows (~100-300MB)
- Complex aggregations: Short-lived DuckDB connections on filtered data

Memory Profile:
- Idle: ~5MB (metadata only)
- Per query: 100-400MB (filtered subset)
- Peak: <500MB (safe for 512MB Render instances)
"""

import streamlit as st
import pandas as pd
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow as pa
import duckdb
import os
from typing import Optional, Dict, Any, List, Tuple


# =============================================================================
# CONFIGURATION
# =============================================================================

# Path to compressed parquet file (set via environment variable or default)
DATA_PATH = os.environ.get('DATA_PATH', 'Data/CSM_Dashboard_compressed.parquet')

# Decoding mappings for encoded columns
DECODING_MAPPINGS = {
    "tipo_territorio": {1: "Municipio", 0: "Departamento", -1: None},
    "predicted_class": {1: "Incluida", 0: "Excluida", -1: None},
    "PDET": {1: 1, 0: 0, -1: None},
    "Grupo_MDM": {0: "C", 1: "G1", 2: "G2", 3: "G3", 4: "G4", 5: "G5", -1: None},
    "Cat_IICA": {4: "Muy Alto", 3: "Alto", 2: "Medio", 1: "Medio Bajo", 0: "Bajo", -1: None},
}

# Encoding mappings (for filter construction)
ENCODING_MAPPINGS = {
    "tipo_territorio": {"Municipio": 1, "Departamento": 0},
    "predicted_class": {"Incluida": 1, "Excluida": 0},
    "Grupo_MDM": {"C": 0, "G1": 1, "G2": 2, "G3": 3, "G4": 4, "G5": 5},
    "Cat_IICA": {"Muy Alto": 4, "Alto": 3, "Medio": 2, "Medio Bajo": 1, "Bajo": 0},
}


# =============================================================================
# PARQUET FILE ACCESS (CACHED METADATA)
# =============================================================================

@st.cache_resource
def get_parquet_file() -> Optional[pq.ParquetFile]:
    """
    Cache ParquetFile object (metadata only - ~5MB memory).

    This function caches only the parquet file metadata, not the actual data.
    The data is loaded on-demand using predicate pushdown filters.

    Returns:
        ParquetFile object or None if file not found
    """
    # Check multiple possible paths
    possible_paths = [
        DATA_PATH,
        'Data/CSM_Dashboard_compressed.parquet',
        'Data/Data Final Dashboard.parquet',
        '/data/CSM_Dashboard_compressed.parquet',  # Render disk mount
    ]

    parquet_path = None
    for path in possible_paths:
        if os.path.exists(path):
            parquet_path = path
            print(f"[PyArrow] Found parquet at: {parquet_path}")
            break

    if parquet_path is None:
        st.error("Parquet file not found. Checked paths:")
        for path in possible_paths:
            st.error(f"  - {path}")
        st.error(f"Current directory: {os.getcwd()}")
        return None

    try:
        pf = pq.ParquetFile(parquet_path)
        num_rows = pf.metadata.num_rows
        num_cols = pf.metadata.num_columns
        print(f"[PyArrow] Loaded metadata: {num_rows:,} rows, {num_cols} columns")
        st.sidebar.success(f"📊 Datos: {num_rows:,} registros")
        return pf
    except Exception as e:
        st.error(f"Error loading parquet file: {str(e)}")
        return None


def _decode_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Decode encoded categorical columns back to their original string values.

    Args:
        df: DataFrame with encoded columns

    Returns:
        DataFrame with decoded columns
    """
    for col, mapping in DECODING_MAPPINGS.items():
        if col in df.columns:
            df[col] = df[col].map(mapping)
    return df


def _build_pyarrow_filter(
    umbral_similitud: float,
    tipo_territorio: str = "Municipio",
    departamento: str = None,
    municipio: str = None,
    solo_politica_publica: bool = True,
    filtro_pdet: str = "Todos",
    filtro_iica: list = None,
    filtro_ipm: tuple = (0.0, 100.0),
    filtro_mdm: list = None,
) -> List:
    """
    Build PyArrow filter expression for predicate pushdown.

    Returns list of tuples for PyArrow's filters parameter.
    """
    filters = []

    # Similarity threshold
    filters.append(('sentence_similarity', '>=', umbral_similitud))

    # Territory type (encoded)
    if tipo_territorio:
        encoded_tipo = ENCODING_MAPPINGS["tipo_territorio"].get(tipo_territorio)
        if encoded_tipo is not None:
            filters.append(('tipo_territorio', '==', encoded_tipo))

    # Department filter
    if departamento and departamento != 'Todos':
        filters.append(('dpto', '==', departamento))

    # Municipality filter
    if municipio and municipio != 'Todos':
        filters.append(('mpio', '==', municipio))

    # PDET filter
    if filtro_pdet == "Solo PDET":
        filters.append(('PDET', '==', 1))
    elif filtro_pdet == "Solo No PDET":
        filters.append(('PDET', '==', 0))

    # IPM filter
    if filtro_ipm and filtro_ipm != (0.0, 100.0):
        filters.append(('IPM_2018', '>=', filtro_ipm[0]))
        filters.append(('IPM_2018', '<=', filtro_ipm[1]))

    return filters


def query_filtered_data(
    umbral_similitud: float,
    departamento: str = None,
    municipio: str = None,
    solo_politica_publica: bool = True,
    limite: int = None,
    filtro_pdet: str = "Todos",
    filtro_iica: list = None,
    filtro_ipm: tuple = (0.0, 100.0),
    filtro_mdm: list = None,
    columns: List[str] = None,
) -> pd.DataFrame:
    """
    Query data with PyArrow predicate pushdown.

    This function reads only the rows matching the filters, minimizing memory usage.

    Args:
        umbral_similitud: Minimum similarity threshold
        departamento: Department filter
        municipio: Municipality filter
        solo_politica_publica: Filter for public policy
        limite: Maximum rows to return
        filtro_pdet: PDET filter
        filtro_iica: IICA categories filter
        filtro_ipm: IPM range filter
        filtro_mdm: MDM groups filter
        columns: Specific columns to load (None = all)

    Returns:
        Filtered DataFrame with decoded columns
    """
    pf = get_parquet_file()
    if pf is None:
        return pd.DataFrame()

    try:
        # Build PyArrow filters
        filters = _build_pyarrow_filter(
            umbral_similitud=umbral_similitud,
            tipo_territorio="Municipio",
            departamento=departamento,
            municipio=municipio,
            solo_politica_publica=solo_politica_publica,
            filtro_pdet=filtro_pdet,
            filtro_iica=filtro_iica,
            filtro_ipm=filtro_ipm,
            filtro_mdm=filtro_mdm,
        )

        # Read with predicate pushdown
        table = pf.read(filters=filters, columns=columns)
        df = table.to_pandas()

        # Decode categorical columns
        df = _decode_columns(df)

        # Apply filters that PyArrow can't handle directly
        if solo_politica_publica:
            df = df[
                (df['predicted_class'] == 'Incluida') |
                ((df['predicted_class'] == 'Excluida') & (df['prediction_confidence'] < 0.8))
            ]

        # IICA filter (list-based, harder for PyArrow)
        if filtro_iica and len(filtro_iica) > 0:
            df = df[df['Cat_IICA'].isin(filtro_iica)]

        # MDM filter (list-based)
        if filtro_mdm and len(filtro_mdm) > 0:
            df = df[df['Grupo_MDM'].isin(filtro_mdm)]

        # Sort and limit
        df = df.sort_values('sentence_similarity', ascending=False)
        if limite:
            df = df.head(limite)

        return df

    except Exception as e:
        st.error(f"Error en consulta filtrada: {str(e)}")
        return pd.DataFrame()


# =============================================================================
# DUCKDB-BASED AGGREGATIONS (SHORT-LIVED CONNECTIONS)
# =============================================================================

def _run_duckdb_query(df: pd.DataFrame, query: str) -> pd.DataFrame:
    """
    Run a DuckDB query on a DataFrame using a short-lived connection.

    This approach uses DuckDB for complex SQL operations but only on
    pre-filtered data, keeping memory usage low.

    Args:
        df: Input DataFrame (should be pre-filtered)
        query: SQL query (use 'df' as table name)

    Returns:
        Query result as DataFrame
    """
    try:
        conn = duckdb.connect(':memory:')
        conn.execute("SET memory_limit='256MB'")
        result = conn.execute(query).df()
        conn.close()
        return result
    except Exception as e:
        st.error(f"Error en consulta DuckDB: {str(e)}")
        return pd.DataFrame()


# =============================================================================
# LEGACY API COMPATIBILITY LAYER
# =============================================================================
# These functions maintain the same interface as the original google_drive_client.py
# so that vista_*.py files don't need modification.


@st.cache_resource
def conectar_duckdb_parquet() -> bool:
    """
    Initialize data connection (compatibility wrapper).

    In the new architecture, this just verifies the parquet file is accessible.
    Returns True if successful (used as a flag, not an actual connection).
    """
    pf = get_parquet_file()
    return pf is not None


def obtener_metadatos_basicos() -> Dict[str, Any]:
    """
    Get basic dataset statistics.

    Returns:
        Dictionary with total counts and averages
    """
    pf = get_parquet_file()
    if pf is None:
        return {}

    try:
        # Read only necessary columns for metadata
        columns = ['dpto', 'mpio_cdpmp', 'recommendation_code', 'sentence_similarity', 'tipo_territorio']
        table = pf.read(
            columns=columns,
            filters=[('tipo_territorio', '==', ENCODING_MAPPINGS["tipo_territorio"]["Municipio"])]
        )
        df = table.to_pandas()

        return {
            'total_registros': len(df),
            'total_departamentos': df['dpto'].nunique(),
            'total_municipios': df['mpio_cdpmp'].nunique(),
            'total_recomendaciones': df['recommendation_code'].nunique(),
            'similitud_promedio': df['sentence_similarity'].mean()
        }
    except Exception as e:
        st.error(f"Error obteniendo metadatos: {str(e)}")
        return {}


def construir_filtros_where(filtro_pdet: str = "Todos",
                            filtro_iica: list = None,
                            filtro_ipm: tuple = (0.0, 100.0),
                            filtro_mdm: list = None) -> str:
    """
    Build WHERE clause for socioeconomic filters (DuckDB SQL).

    This is kept for compatibility with complex aggregation queries.
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
    Query filtered data (compatibility wrapper).
    """
    return query_filtered_data(
        umbral_similitud=umbral_similitud,
        departamento=departamento,
        municipio=municipio,
        solo_politica_publica=solo_politica_publica,
        limite=limite,
        filtro_pdet=filtro_pdet,
        filtro_iica=filtro_iica,
        filtro_ipm=filtro_ipm,
        filtro_mdm=filtro_mdm,
    )


def obtener_estadisticas_departamentales(umbral_similitud: float,
                                         filtro_pdet: str = "Todos",
                                         filtro_iica: list = None,
                                         filtro_ipm: tuple = (0.0, 100.0),
                                         filtro_mdm: list = None) -> pd.DataFrame:
    """
    Calculate departmental statistics with min/max municipalities.
    """
    # Get filtered data first (uses PyArrow predicate pushdown)
    df = query_filtered_data(
        umbral_similitud=umbral_similitud,
        solo_politica_publica=False,
        filtro_pdet=filtro_pdet,
        filtro_iica=filtro_iica,
        filtro_ipm=filtro_ipm,
        filtro_mdm=filtro_mdm,
    )

    if df.empty:
        return pd.DataFrame()

    # Use DuckDB for complex aggregation on pre-filtered data
    query = """
        WITH recomendaciones_por_municipio AS (
            SELECT
                dpto_cdpmp,
                dpto,
                mpio_cdpmp,
                mpio,
                COUNT(DISTINCT recommendation_code) as num_recomendaciones
            FROM df
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

    return _run_duckdb_query(df, query)


def obtener_ranking_municipios(umbral_similitud: float,
                               solo_politica_publica: bool = True,
                               top_n: int = None,
                               filtro_pdet: str = "Todos",
                               filtro_iica: list = None,
                               filtro_ipm: tuple = (0.0, 100.0),
                               filtro_mdm: list = None) -> pd.DataFrame:
    """
    Generate municipality ranking by recommendations implemented.
    """
    df = query_filtered_data(
        umbral_similitud=umbral_similitud,
        solo_politica_publica=solo_politica_publica,
        filtro_pdet=filtro_pdet,
        filtro_iica=filtro_iica,
        filtro_ipm=filtro_ipm,
        filtro_mdm=filtro_mdm,
    )

    if df.empty:
        return pd.DataFrame()

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
        FROM df
        GROUP BY mpio_cdpmp, mpio, dpto
        ORDER BY Recomendaciones_Implementadas DESC
        {limit_clause}
    """

    return _run_duckdb_query(df, query)


def obtener_top_recomendaciones(umbral_similitud: float = 0.6,
                                departamento: str = None,
                                municipio: str = None,
                                limite: int = 10,
                                filtro_pdet: str = "Todos",
                                filtro_iica: list = None,
                                filtro_ipm: tuple = (0.0, 100.0),
                                filtro_mdm: list = None) -> pd.DataFrame:
    """
    Get most frequently mentioned recommendations.
    """
    df = query_filtered_data(
        umbral_similitud=umbral_similitud,
        departamento=departamento,
        municipio=municipio,
        solo_politica_publica=False,
        filtro_pdet=filtro_pdet,
        filtro_iica=filtro_iica,
        filtro_ipm=filtro_ipm,
        filtro_mdm=filtro_mdm,
    )

    if df.empty:
        return pd.DataFrame()

    query = f"""
        SELECT
            recommendation_code as Codigo,
            recommendation_text as Texto,
            recommendation_priority as Prioridad,
            COUNT(*) as Frecuencia_Oraciones,
            COUNT(DISTINCT mpio_cdpmp) as Municipios_Implementan,
            AVG(sentence_similarity) as Similitud_Promedio
        FROM df
        GROUP BY recommendation_code, recommendation_text, recommendation_priority
        ORDER BY Frecuencia_Oraciones DESC
        LIMIT {limite}
    """

    return _run_duckdb_query(df, query)


def obtener_municipios_por_recomendacion(codigo_recomendacion: str,
                                         umbral_similitud: float = 0.6,
                                         limite: int = 100,
                                         filtro_pdet: str = "Todos",
                                         filtro_iica: list = None,
                                         filtro_ipm: tuple = (0.0, 100.0),
                                         filtro_mdm: list = None) -> pd.DataFrame:
    """
    Get municipalities mentioning a specific recommendation.
    """
    pf = get_parquet_file()
    if pf is None:
        return pd.DataFrame()

    try:
        # Read with filters including recommendation code
        filters = [
            ('sentence_similarity', '>=', umbral_similitud),
            ('tipo_territorio', '==', ENCODING_MAPPINGS["tipo_territorio"]["Municipio"]),
            ('recommendation_code', '==', codigo_recomendacion),
        ]

        # Add PDET filter
        if filtro_pdet == "Solo PDET":
            filters.append(('PDET', '==', 1))
        elif filtro_pdet == "Solo No PDET":
            filters.append(('PDET', '==', 0))

        # Add IPM filters
        if filtro_ipm and filtro_ipm != (0.0, 100.0):
            filters.append(('IPM_2018', '>=', filtro_ipm[0]))
            filters.append(('IPM_2018', '<=', filtro_ipm[1]))

        table = pf.read(filters=filters)
        df = table.to_pandas()
        df = _decode_columns(df)

        # Apply list-based filters
        if filtro_iica and len(filtro_iica) > 0:
            df = df[df['Cat_IICA'].isin(filtro_iica)]

        if filtro_mdm and len(filtro_mdm) > 0:
            df = df[df['Grupo_MDM'].isin(filtro_mdm)]

        if df.empty:
            return pd.DataFrame()

        query = f"""
            SELECT
                mpio as Municipio,
                dpto as Departamento,
                COUNT(*) as Frecuencia_Oraciones,
                AVG(sentence_similarity) as Similitud_Promedio,
                MAX(sentence_similarity) as Similitud_Maxima
            FROM df
            GROUP BY mpio, dpto
            ORDER BY Frecuencia_Oraciones DESC, Similitud_Promedio DESC
            LIMIT {limite}
        """

        return _run_duckdb_query(df, query)

    except Exception as e:
        st.error(f"Error obteniendo municipios por recomendación: {str(e)}")
        return pd.DataFrame()


def obtener_todos_los_municipios() -> pd.DataFrame:
    """
    Get list of all unique municipalities.
    """
    pf = get_parquet_file()
    if pf is None:
        return pd.DataFrame()

    try:
        columns = ['mpio_cdpmp', 'mpio', 'dpto_cdpmp', 'dpto', 'tipo_territorio']
        table = pf.read(
            columns=columns,
            filters=[('tipo_territorio', '==', ENCODING_MAPPINGS["tipo_territorio"]["Municipio"])]
        )
        df = table.to_pandas()
        df = _decode_columns(df)

        # Get unique municipalities
        df_unique = df.drop_duplicates(subset=['mpio_cdpmp']).copy()
        df_unique = df_unique.rename(columns={'mpio': 'Municipio', 'dpto': 'Departamento'})
        df_unique = df_unique[['mpio_cdpmp', 'Municipio', 'dpto_cdpmp', 'Departamento']]
        df_unique = df_unique.sort_values(['Departamento', 'Municipio'])

        return df_unique

    except Exception as e:
        st.error(f"Error obteniendo lista de municipios: {str(e)}")
        return pd.DataFrame()


def obtener_todos_los_departamentos() -> pd.DataFrame:
    """
    Get list of all unique departments.
    """
    pf = get_parquet_file()
    if pf is None:
        return pd.DataFrame()

    try:
        columns = ['dpto_cdpmp', 'dpto', 'tipo_territorio']
        table = pf.read(
            columns=columns,
            filters=[('tipo_territorio', '==', ENCODING_MAPPINGS["tipo_territorio"]["Municipio"])]
        )
        df = table.to_pandas()
        df = _decode_columns(df)

        # Get unique departments
        df_unique = df.drop_duplicates(subset=['dpto_cdpmp']).copy()
        df_unique = df_unique.rename(columns={'dpto': 'Departamento'})
        df_unique = df_unique[['dpto_cdpmp', 'Departamento']]
        df_unique = df_unique.sort_values('Departamento')

        return df_unique

    except Exception as e:
        st.error(f"Error obteniendo lista de departamentos: {str(e)}")
        return pd.DataFrame()
