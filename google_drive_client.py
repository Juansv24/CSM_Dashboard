import streamlit as st
import pandas as pd
import gdown
import duckdb
import os
import tempfile
from typing import Optional, Dict, Any


@st.cache_resource
def conectar_duckdb_parquet() -> Optional[duckdb.DuckDBPyConnection]:
    """
    Descargar Parquet desde Google Drive y crear conexión DuckDB en memoria
    """

    try:
        # Obtener ID del archivo Parquet
        try:
            file_id = st.secrets["PARQUET_FILE_ID"]
        except Exception:
            st.error("⚠️ Configura PARQUET_FILE_ID en Streamlit secrets")
            return None

        # Configurar archivo temporal
        temp_dir = tempfile.gettempdir()
        parquet_path = os.path.join(temp_dir, f"dashboard_{file_id[:8]}.parquet")

        # Descargar si no existe
        if not os.path.exists(parquet_path):
            url = f"https://drive.google.com/uc?id={file_id}"
            with st.spinner('📥 Descargando datos...'):
                gdown.download(url, parquet_path, quiet=False)
            st.success("✅ Datos descargados")
        else:
            st.info("⚡ Usando datos en caché")

        # Crear conexión DuckDB en memoria
        conn = duckdb.connect(':memory:')
        conn.execute("SET memory_limit='1GB'")
        conn.execute("SET threads=2")

        # Crear vista desde Parquet
        conn.execute(f"""
            CREATE VIEW datos_principales AS 
            SELECT * FROM read_parquet('{parquet_path}')
        """)

        # Verificar conexión
        total = conn.execute("SELECT COUNT(*) FROM datos_principales").fetchone()[0]
        st.sidebar.success(f"📊 Registros: {total:,}")

        return conn

    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        return None


@st.cache_data
def obtener_metadatos_basicos() -> Dict[str, Any]:
    """Obtener métricas básicas"""

    try:
        conn = st.session_state.get('duckdb_conn')
        if not conn:
            conn = conectar_duckdb_parquet()
            st.session_state.duckdb_conn = conn

        if not conn:
            return {}

        resultado = conn.execute("""
            SELECT 
                COUNT(*) as total_registros,
                COUNT(DISTINCT dpto) as total_departamentos,
                COUNT(DISTINCT mpio_cdpmp) as total_municipios,  -- Changed from mpio to mpio_cdpmp
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
        st.error(f"Error metadatos: {str(e)}")
        return {}


def consultar_datos_filtrados(umbral_similitud: float,
                              departamento: str = None,
                              municipio: str = None,
                              solo_politica_publica: bool = True,
                              limite: int = None,
                              filtro_pdet: str = "Todos",
                              filtro_iica: list = None,
                              filtro_ipm: tuple = (0.0, 100.0),
                              filtro_mdm: list = None) -> pd.DataFrame:
    """Consultar datos con filtros"""

    try:
        conn = st.session_state.get('duckdb_conn')
        if not conn:
            conn = conectar_duckdb_parquet()
            st.session_state.duckdb_conn = conn

        if not conn:
            return pd.DataFrame()

        where_conditions = [
            f"sentence_similarity >= {umbral_similitud}",
            "tipo_territorio = 'Municipio'"
        ]

        # Filtro de política pública
        if solo_politica_publica:
            where_conditions.append(
                "(predicted_class = 'Incluida' OR " +
                "(predicted_class = 'Excluida' AND prediction_confidence < 0.8))"
            )

        # Filtro por departamento
        if departamento and departamento != 'Todos':
            departamento_escaped = departamento.replace("'", "''")
            where_conditions.append(f"dpto = '{departamento_escaped}'")

        # Filtro por municipio
        if municipio and municipio != 'Todos':
            municipio_escaped = municipio.replace("'", "''")
            where_conditions.append(f"mpio = '{municipio_escaped}'")

        # Filtros socioeconómicos
        # Filtro PDET
        if filtro_pdet == "Solo PDET":
            where_conditions.append("PDET = 1")
        elif filtro_pdet == "Solo No PDET":
            where_conditions.append("PDET = 0")

        # Filtro IICA
        if filtro_iica and len(filtro_iica) > 0:
            iica_list = "','".join(filtro_iica)
            where_conditions.append(f"Cat_IICA IN ('{iica_list}')")

        # Filtro IPM
        if filtro_ipm != (0.0, 100.0):
            where_conditions.append(f"IPM_2018 BETWEEN {filtro_ipm[0]} AND {filtro_ipm[1]}")

        # Filtro MDM
        if filtro_mdm and len(filtro_mdm) > 0:
            mdm_list = "','".join(filtro_mdm)
            where_conditions.append(f"Grupo_MDM IN ('{mdm_list}')")

        where_clause = " AND ".join(where_conditions)
        limit_clause = f"LIMIT {limite}" if limite else ""

        query = f"""
            SELECT * FROM datos_principales 
            WHERE {where_clause}
            ORDER BY sentence_similarity DESC
            {limit_clause}
        """

        return conn.execute(query).df()

    except Exception as e:
        st.error(f"Error consulta: {str(e)}")
        return pd.DataFrame()

def obtener_estadisticas_departamentales(umbral_similitud: float,
                                         filtro_pdet: str = "Todos",
                                         filtro_iica: list = None,
                                         filtro_ipm: tuple = (0.0, 100.0),
                                         filtro_mdm: list = None) -> pd.DataFrame:
    """Estadísticas por departamento - Con MIN, MAX y nombres de municipios"""

    try:
        conn = st.session_state.get('duckdb_conn')
        if not conn:
            return pd.DataFrame()

        # Construir condiciones de filtro
        filtro_conditions = []

        # Filtro PDET
        if filtro_pdet == "Solo PDET":
            filtro_conditions.append("PDET = 1")
        elif filtro_pdet == "Solo No PDET":
            filtro_conditions.append("PDET = 0")

        # Filtro IICA
        if filtro_iica and len(filtro_iica) > 0:
            iica_list = "','".join(filtro_iica)
            filtro_conditions.append(f"Cat_IICA IN ('{iica_list}')")

        # Filtro IPM
        if filtro_ipm != (0.0, 100.0):
            filtro_conditions.append(f"IPM_2018 BETWEEN {filtro_ipm[0]} AND {filtro_ipm[1]}")

        # Filtro MDM
        if filtro_mdm and len(filtro_mdm) > 0:
            mdm_list = "','".join(filtro_mdm)
            filtro_conditions.append(f"Grupo_MDM IN ('{mdm_list}')")

        # Combinar condiciones
        additional_where = ""
        if filtro_conditions:
            additional_where = " AND " + " AND ".join(filtro_conditions)

        query = f"""
            WITH recomendaciones_por_municipio AS (
                -- Paso 1: Contar cuántas recomendaciones menciona cada municipio
                SELECT 
                    dpto_cdpmp,
                    dpto,
                    mpio_cdpmp,
                    mpio,
                    COUNT(DISTINCT recommendation_code) as num_recomendaciones
                FROM datos_principales 
                WHERE tipo_territorio = 'Municipio'
                AND sentence_similarity >= {umbral_similitud}
                {additional_where}
                GROUP BY dpto_cdpmp, dpto, mpio_cdpmp, mpio
            ),
            -- Paso 2: Identificar municipios con min y max por departamento
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
            -- Paso 3: Calcular estadísticas generales
            stats_generales AS (
                SELECT 
                    dpto_cdpmp,
                    dpto as Departamento,
                    COUNT(DISTINCT mpio_cdpmp) as Municipios,
                    ROUND(AVG(num_recomendaciones), 0) as Promedio_Recomendaciones
                FROM recomendaciones_por_municipio
                GROUP BY dpto_cdpmp, dpto
            )
            -- Paso 4: Unir todo
            SELECT 
                s.dpto_cdpmp,
                s.Departamento,
                s.Municipios,
                s.Promedio_Recomendaciones,
                mn.Min_Recomendaciones,
                mn.Municipio_Min,
                mx.Max_Recomendaciones,
                mx.Municipio_Max
            FROM stats_generales s
            LEFT JOIN municipio_min mn ON s.dpto_cdpmp = mn.dpto_cdpmp
            LEFT JOIN municipio_max mx ON s.dpto_cdpmp = mx.dpto_cdpmp
            ORDER BY s.Promedio_Recomendaciones DESC
        """

        return conn.execute(query).df()

    except Exception as e:
        st.error(f"Error estadísticas departamentales: {str(e)}")
        import traceback
        st.error(traceback.format_exc())
        return pd.DataFrame()


def obtener_ranking_municipios(umbral_similitud: float,
                               solo_politica_publica: bool = True,
                               top_n: int = None,
                               filtro_pdet: str = "Todos",
                               filtro_iica: list = None,
                               filtro_ipm: tuple = (0.0, 100.0),
                               filtro_mdm: list = None) -> pd.DataFrame:
    """Ranking de municipios"""

    try:
        conn = st.session_state.get('duckdb_conn')
        if not conn:
            return pd.DataFrame()

        # Filtro de política pública
        policy_condition = ""
        if solo_politica_publica:
            policy_condition = """
                AND (predicted_class = 'Incluida' OR 
                     (predicted_class = 'Excluida' AND prediction_confidence < 0.8))
            """

        # Construir condiciones de filtro socioeconómico
        filtro_conditions = []

        # Filtro PDET
        if filtro_pdet == "Solo PDET":
            filtro_conditions.append("PDET = 1")
        elif filtro_pdet == "Solo No PDET":
            filtro_conditions.append("PDET = 0")

        # Filtro IICA
        if filtro_iica and len(filtro_iica) > 0:
            iica_list = "','".join(filtro_iica)
            filtro_conditions.append(f"Cat_IICA IN ('{iica_list}')")

        # Filtro IPM
        if filtro_ipm != (0.0, 100.0):
            filtro_conditions.append(f"IPM_2018 BETWEEN {filtro_ipm[0]} AND {filtro_ipm[1]}")

        # Filtro MDM
        if filtro_mdm and len(filtro_mdm) > 0:
            mdm_list = "','".join(filtro_mdm)
            filtro_conditions.append(f"Grupo_MDM IN ('{mdm_list}')")

        # Combinar condiciones adicionales
        additional_where = ""
        if filtro_conditions:
            additional_where = " AND " + " AND ".join(filtro_conditions)

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
            WHERE sentence_similarity >= {umbral_similitud}
            AND tipo_territorio = 'Municipio'
            {policy_condition}
            {additional_where}
            GROUP BY mpio_cdpmp, mpio, dpto
            ORDER BY Recomendaciones_Implementadas DESC
            {limit_clause}
        """

        return conn.execute(query).df()

    except Exception as e:
        st.error(f"Error ranking municipios: {str(e)}")
        return pd.DataFrame()


def obtener_top_recomendaciones(umbral_similitud: float = 0.6,
                                departamento: str = None,
                                municipio: str = None,
                                limite: int = 10,
                                filtro_pdet: str = "Todos",
                                filtro_iica: list = None,
                                filtro_ipm: tuple = (0.0, 100.0),
                                filtro_mdm: list = None) -> pd.DataFrame:
    """Top recomendaciones"""

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

        # Agregar filtros socioeconómicos
        # Filtro PDET
        if filtro_pdet == "Solo PDET":
            where_conditions.append("PDET = 1")
        elif filtro_pdet == "Solo No PDET":
            where_conditions.append("PDET = 0")

        # Filtro IICA
        if filtro_iica and len(filtro_iica) > 0:
            iica_list = "','".join(filtro_iica)
            where_conditions.append(f"Cat_IICA IN ('{iica_list}')")

        # Filtro IPM
        if filtro_ipm != (0.0, 100.0):
            where_conditions.append(f"IPM_2018 BETWEEN {filtro_ipm[0]} AND {filtro_ipm[1]}")

        # Filtro MDM
        if filtro_mdm and len(filtro_mdm) > 0:
            mdm_list = "','".join(filtro_mdm)
            where_conditions.append(f"Grupo_MDM IN ('{mdm_list}')")

        where_clause = " AND ".join(where_conditions)

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
        st.error(f"Error top recomendaciones: {str(e)}")
        return pd.DataFrame()


def obtener_municipios_por_recomendacion(codigo_recomendacion: str,
                                         umbral_similitud: float = 0.6,
                                         limite: int = 20,
                                         filtro_pdet: str = "Todos",
                                         filtro_iica: list = None,
                                         filtro_ipm: tuple = (0.0, 100.0),
                                         filtro_mdm: list = None) -> pd.DataFrame:
    """Municipios que más implementan una recomendación específica"""

    try:
        conn = st.session_state.get('duckdb_conn')
        if not conn:
            return pd.DataFrame()

        codigo_escaped = codigo_recomendacion.replace("'", "''")

        # Construir condiciones de filtro
        filtro_conditions = []

        # Filtro PDET
        if filtro_pdet == "Solo PDET":
            filtro_conditions.append("PDET = 1")
        elif filtro_pdet == "Solo No PDET":
            filtro_conditions.append("PDET = 0")

        # Filtro IICA
        if filtro_iica and len(filtro_iica) > 0:
            iica_list = "','".join(filtro_iica)
            filtro_conditions.append(f"Cat_IICA IN ('{iica_list}')")

        # Filtro IPM
        if filtro_ipm != (0.0, 100.0):
            filtro_conditions.append(f"IPM_2018 BETWEEN {filtro_ipm[0]} AND {filtro_ipm[1]}")

        # Filtro MDM
        if filtro_mdm and len(filtro_mdm) > 0:
            mdm_list = "','".join(filtro_mdm)
            filtro_conditions.append(f"Grupo_MDM IN ('{mdm_list}')")

        # Combinar condiciones
        additional_where = ""
        if filtro_conditions:
            additional_where = " AND " + " AND ".join(filtro_conditions)

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
            {additional_where}
            GROUP BY mpio, dpto
            ORDER BY Frecuencia_Oraciones DESC, Similitud_Promedio DESC
            LIMIT {limite}
        """

        return conn.execute(query).df()

    except Exception as e:
        st.error(f"Error municipios por recomendación: {str(e)}")
        return pd.DataFrame()

def obtener_listas_departamentos_municipios() -> tuple[list, dict]:
    """Listas de territorios usando códigos únicos"""
    try:
        conn = st.session_state.get('duckdb_conn')
        if not conn:
            return [], {}

        # Obtener departamentos únicos por código
        departamentos_df = conn.execute("""
            SELECT DISTINCT dpto_cdpmp, dpto 
            FROM datos_principales 
            ORDER BY dpto
        """).df()

        departamentos = departamentos_df['dpto'].tolist()

        # Obtener municipios por departamento usando códigos
        municipios_por_depto = {}
        for _, row in departamentos_df.iterrows():
            dpto_codigo = row['dpto_cdpmp']
            dpto_nombre = row['dpto']

            municipios_df = conn.execute(f"""
                SELECT DISTINCT mpio_cdpmp, mpio 
                FROM datos_principales 
                WHERE dpto_cdpmp = '{dpto_codigo}'
                ORDER BY mpio
            """).df()

            municipios_por_depto[dpto_nombre] = municipios_df['mpio'].tolist()

        return departamentos, municipios_por_depto

    except Exception as e:
        st.error(f"Error listas territorios: {str(e)}")
        return [], {}

def obtener_todos_los_municipios() -> pd.DataFrame:
    """Obtener todos los municipios únicos disponibles en la muestra"""
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
        st.error(f"Error obteniendo municipios: {str(e)}")
        return pd.DataFrame()

def obtener_todos_los_departamentos() -> pd.DataFrame:
    """Obtener todos los departamentos únicos disponibles en la muestra"""
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
        st.error(f"Error obteniendo departamentos: {str(e)}")
        return pd.DataFrame()

# Función de compatibilidad
def cargar_datos_optimizado() -> Optional[pd.DataFrame]:
    """Función de compatibilidad"""

    try:
        conn = st.session_state.get('duckdb_conn')
        if not conn:
            conn = conectar_duckdb_parquet()
            st.session_state.duckdb_conn = conn

        if not conn:
            return None

        return conn.execute("SELECT * FROM datos_principales LIMIT 100").df()

    except Exception as e:
        st.error(f"Error compatibilidad: {str(e)}")
        return None