# Dashboard de Similitudes Jerárquicas - CEV Colombia

Sistema que identifica mediante algoritmos de similitud semántica la mención de 75 recomendaciones de la Comisión para el Esclarecimiento de la Verdad (CEV) en los planes de desarrollo territorial de 1.028 municipios y 33 departamentos en Colombia.

## 📋 Tabla de Contenidos

- [Descripción General](#descripción-general)
- [Características Principales](#características-principales)
- [Arquitectura del Sistema](#arquitectura-del-sistema)
- [Instalación](#instalación)
- [Configuración](#configuración)
- [Uso del Dashboard](#uso-del-dashboard)
- [Módulos del Sistema](#módulos-del-sistema)
- [Actualización de Datos](#actualización-de-datos)
- [Personalización](#personalización)
- [Troubleshooting](#troubleshooting)
- [Contribuir](#contribuir)
- [Licencia](#licencia)

---

## 🎯 Descripción General

Este dashboard interactivo permite:

- **Identificar** menciones de recomendaciones CEV en planes de desarrollo territorial
- **Visualizar** patrones de implementación a nivel nacional, departamental y municipal
- **Analizar** similitud semántica entre recomendaciones y políticas públicas territoriales
- **Comparar** niveles de implementación entre territorios

### ¿Qué es la similitud semántica?

Es una medida (0-1) que indica qué tan parecido es el significado entre dos textos, independientemente de las palabras utilizadas. El sistema evalúa qué tan parecido es el contenido de una recomendación CEV con oraciones de un Plan de Desarrollo Territorial.

**Escala de interpretación:**
- 🟢 **0.80-1.00:** La oración menciona prácticamente lo mismo que la recomendación
- 🟡 **0.65-0.79:** El concepto de la recomendación está claramente presente
- 🟠 **0.50-0.64:** Hay elementos relacionados pero menos directos

---

## ✨ Características Principales

### 📊 Vista General (Nacional)
- Mapa coroplético interactivo con estadísticas departamentales
- Vista detallada municipal al seleccionar un departamento
- Top 10 recomendaciones más mencionadas
- Análisis de dispersión: Recomendaciones vs. Frecuencia
- Exploración por recomendación específica con paginación

### 🏢 Vista Departamental
- Análisis de planes de desarrollo departamentales
- Ranking departamental de implementación
- Top 5 recomendaciones más frecuentes
- Análisis por tema
- Exploración jerárquica (Párrafos/Oraciones)
- Diccionario completo de recomendaciones

### 🏛️ Vista Municipal
- Información socioeconómica (IPM, PDET, IICA, MDM)
- Ranking municipal nacional
- Análisis de implementación por tema
- Navegación jerárquica multinivel
- Búsqueda y filtrado avanzado
- Descarga de datos en CSV

### 🔍 Filtros Globales
- **Umbral de similitud:** 0.5 - 1.0 (recomendado: 0.65)
- **PDET:** Todos / Solo PDET / Solo No PDET
- **Categoría IICA:** Muy Alto, Alto, Medio, Bajo, Medio Bajo
- **Rango IPM:** 0 - 100
- **Grupo MDM:** C, G1, G2, G3, G4, G5

---

## 🏗️ Arquitectura del Sistema

### Estructura de Archivos

```
📦 dashboard-cev-colombia
├── 📄 app.py                          # Punto de entrada principal
├── 📄 google_drive_client.py          # Capa de datos y consultas DuckDB
├── 📄 vista_general.py                # Vista nacional agregada
├── 📄 vista_departamental.py          # Análisis departamental
├── 📄 vista_municipal.py              # Análisis municipal detallado
├── 📄 Mapa Municipios.geojson         # Geometrías municipales
├── 📄 requirements.txt                # Dependencias Python
├── 📄 .gitignore                      # Archivos excluidos de Git
├── 📁 .streamlit/
│   └── 📄 secrets.toml                # Configuración sensible (NO subir a Git)
└── 📄 README.md                       # Este archivo
```

### Stack Tecnológico

- **Frontend:** Streamlit 1.30+
- **Base de datos:** DuckDB (in-memory)
- **Almacenamiento:** Google Drive (Parquet)
- **Visualización:** Plotly Express
- **Procesamiento:** Pandas, NumPy

---

## 🚀 Instalación

### Requisitos Previos

- Python 3.8 o superior
- pip (gestor de paquetes)
- Cuenta Google Drive (para almacenar datos)

### Paso 1: Clonar el Repositorio

```bash
git clone https://github.com/tu-usuario/dashboard-cev-colombia.git
cd dashboard-cev-colombia
```

### Paso 2: Crear Entorno Virtual (Recomendado)

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS/Linux
python3 -m venv venv
source venv/bin/activate
```

### Paso 3: Instalar Dependencias

```bash
pip install -r requirements.txt
```

**Contenido de `requirements.txt`:**
```txt
streamlit>=1.30.0
pandas>=2.0.0
duckdb>=0.9.0
gdown>=4.7.0
plotly>=5.17.0
requests>=2.31.0
openpyxl>=3.1.0
```

---

## ⚙️ Configuración

### 1. Configurar Secrets de Streamlit

Crea el archivo `.streamlit/secrets.toml`:

```bash
mkdir .streamlit
touch .streamlit/secrets.toml
```

Añade tu configuración:

```toml
# .streamlit/secrets.toml
PARQUET_FILE_ID = "TU_GOOGLE_DRIVE_FILE_ID_AQUI"
```

#### ¿Cómo obtener el FILE_ID?

1. Sube tu archivo `.parquet` a Google Drive
2. Haz clic derecho → **"Obtener enlace"** → **"Cualquier persona con el enlace"**
3. Copia el enlace: `https://drive.google.com/file/d/1a2B3c4D5e6F7g8H9i0J/view`
4. El `FILE_ID` es: `1a2B3c4D5e6F7g8H9i0J`

### 2. Añadir Archivo GeoJSON

Coloca `Mapa Municipios.geojson` en el directorio raíz del proyecto.

**Estructura requerida:**
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {
        "DPTO_CCDGO": "05",
        "MPIO_CCNCT": "05001",
        "MPIO_CNMBR": "Medellín"
      },
      "geometry": {
        "type": "Polygon",
        "coordinates": [[[-75.5, 6.2], ...]]
      }
    }
  ]
}
```

### 3. Configurar .gitignore

```bash
# .gitignore
.streamlit/secrets.toml
*.parquet
__pycache__/
*.pyc
.DS_Store
venv/
```

---

## 🎮 Uso del Dashboard

### Iniciar el Dashboard

```bash
streamlit run app.py
```

El dashboard se abrirá automáticamente en `http://localhost:8501`

### Navegación

#### Sidebar (Panel Izquierdo)

**Filtros Globales:**
- 🎚️ **Umbral de similitud:** Ajusta la sensibilidad del filtro
- 🏘️ **Filtros socioeconómicos:** PDET, IICA, IPM, MDM

**Información del Proyecto:**
- Objetivo del dashboard
- Instrucciones de uso
- Guía de interpretación

#### Vistas Principales

**🌎 Vista General:**
1. Navega por el mapa interactivo
2. Haz clic en un departamento para ver detalle municipal
3. Explora estadísticas nacionales
4. Analiza recomendaciones específicas

**🏢 Vista Departamental:**
1. Selecciona un departamento en el sidebar
2. Revisa métricas de implementación
3. Explora análisis por párrafos/oraciones
4. Consulta el diccionario de recomendaciones

**🏛️ Vista Municipal:**
1. Selecciona departamento y municipio
2. Revisa información socioeconómica
3. Analiza recomendaciones implementadas
4. Explora contenido jerárquicamente

---

## 📚 Módulos del Sistema

### 1. `app.py` - Controlador Principal

**Responsabilidades:**
- Configuración de página Streamlit
- Inicialización de conexión DuckDB
- Orquestación de vistas
- Gestión de sesión

**Flujo de ejecución:**
```python
main()
  ├── conectar_duckdb_parquet()
  ├── obtener_metadatos_basicos()
  └── render_vista_[general|departamental|municipal]()
```

### 2. `google_drive_client.py` - Capa de Datos

**Funciones principales:**

| Función | Propósito | Retorna |
|---------|-----------|---------|
| `conectar_duckdb_parquet()` | Descarga Parquet y crea conexión | DuckDBPyConnection |
| `obtener_metadatos_basicos()` | Estadísticas generales | Dict |
| `consultar_datos_filtrados()` | Query con filtros aplicados | DataFrame |
| `obtener_estadisticas_departamentales()` | Agregaciones departamentales | DataFrame |
| `obtener_ranking_municipios()` | Ranking nacional | DataFrame |
| `obtener_top_recomendaciones()` | Top N recomendaciones | DataFrame |
| `obtener_municipios_por_recomendacion()` | Municipios por rec. | DataFrame |
| `construir_filtros_where()` | Constructor SQL | String |

**Ejemplo de uso:**
```python
# Obtener datos filtrados
datos = consultar_datos_filtrados(
    umbral_similitud=0.65,
    departamento="Antioquia",
    municipio="Medellín",
    solo_politica_publica=True,
    filtro_pdet="Solo PDET",
    filtro_iica=["Alto", "Muy Alto"],
    filtro_ipm=(20.0, 80.0),
    filtro_mdm=["G3", "G4"]
)
```

### 3. `vista_general.py` - Vista Nacional

**Componentes:**

#### Mapa Coroplético
```python
_render_choropleth_map(dept_data, geojson_data, min_similarity)
```
- Visualización departamental con promedio de recomendaciones
- Click interactivo para ver detalle municipal

#### Estadísticas
```python
_render_recommendations_stats(umbral, metadatos, filtros)
```
- Recomendaciones mencionadas vs. total
- Tasa de mención nacional
- Barra de progreso visual

#### Análisis de Implementación
```python
_render_implementation_analysis(umbral, filtros)
```
- Top 10 recomendaciones (gráfico de barras)
- Scatter plot comparativo

#### Análisis Detallado
```python
_render_detailed_analysis(umbral, filtros)
```
- Selector de recomendación
- Lista paginada de municipios
- Búsqueda y filtrado

### 4. `vista_departamental.py` - Vista Departamental

**Características especiales:**
- Filtra por `tipo_territorio = 'Departamento'`
- **No aplica** filtros socioeconómicos (solo disponibles para municipios)

**Secciones principales:**

```python
_render_vista_departamento_especifico()
  ├── Encabezado con nombre del departamento
  ├── _render_analisis_implementacion_departamento()
  │   ├── Métricas clave (Ranking, Recomendaciones, Prioritarias)
  │   ├── Top 5 recomendaciones por frecuencia
  │   └── Implementación por tema
  ├── _render_analisis_detallado_recomendaciones()
  │   ├── Selector de recomendación
  │   └── Pestañas jerárquicas:
  │       ├── 📄 Párrafos (con paginación)
  │       └── 💬 Oraciones (con paginación)
  └── _render_diccionario_recomendaciones()
      ├── Búsqueda y filtrado
      ├── Tabla interactiva
      └── Vista detallada expandible
```

### 5. `vista_municipal.py` - Vista Municipal

**Información contextual:**
- **IPM 2018:** Índice de Pobreza Multidimensional (0-100, mayor = más pobreza)
- **PDET:** Programa de Desarrollo con Enfoque Territorial (Sí/No)
- **Cat_IICA:** Categoría de Incidencia del Conflicto Armado
- **Grupo_MDM:** Capacidades Iniciales (C=mayor capacidad, G5=menor)

**Análisis jerárquico:**

```
Municipio
  ├── Información Socioeconómica
  ├── Análisis de Implementación
  │   ├── Ranking nacional
  │   ├── Recomendaciones implementadas (X/75)
  │   ├── Prioritarias implementadas (X/45)
  │   ├── Top 5 recomendaciones
  │   └── Implementación por tema
  ├── Análisis Detallado
  │   └── Por cada recomendación:
  │       ├── 📄 Nivel Párrafo
  │       │   ├── ID Párrafo
  │       │   ├── Similitud promedio
  │       │   ├── Número de oraciones
  │       │   └── Texto completo
  │       └── 💬 Nivel Oración
  │           ├── ID Oración
  │           ├── Similitud individual
  │           ├── Clasificación ML
  │           └── Texto completo
  └── Diccionario de Recomendaciones
      ├── Búsqueda
      ├── Filtros (tema, prioridad)
      └── Descarga CSV
```

**Paginación:**
```python
# Configuración por defecto
coincidencias_por_pagina = 5

# Gestión de estado
pagina_key = f'pagina_{contexto}_{identificador}'
st.session_state[pagina_key] = pagina_actual
```

---

## 🔄 Actualización de Datos

### Esquema del Archivo Parquet

#### Columnas Obligatorias

| Columna | Tipo | Descripción | Ejemplo |
|---------|------|-------------|---------|
| `mpio_cdpmp` | string | Código DANE municipio (5 dígitos) | "05001" |
| `mpio` | string | Nombre del municipio | "Medellín" |
| `dpto_cdpmp` | string | Código DANE departamento (2 dígitos) | "05" |
| `dpto` | string | Nombre del departamento | "Antioquia" |
| `tipo_territorio` | string | Tipo de entidad | "Municipio" o "Departamento" |
| `recommendation_code` | string | Código de recomendación CEV | "MCV1" |
| `recommendation_text` | string | Texto completo de la recomendación | "Diseñar e implementar..." |
| `recommendation_topic` | string | Tema/categoría | "Género y diversidad" |
| `recommendation_priority` | int | Priorizada por Gobierno Nacional | 0 o 1 |
| `sentence_text` | string | Texto de la oración del PDT | "Gestión de acciones..." |
| `sentence_similarity` | float | Similitud semántica oración-rec. | 0.0 - 1.0 |
| `paragraph_text` | string | Texto del párrafo completo | "..." |
| `paragraph_similarity` | float | Similitud semántica párrafo-rec. | 0.0 - 1.0 |
| `paragraph_id` | string | ID único del párrafo | "P1234" |
| `page_number` | int | Número de página del documento | 1, 2, 3... |
| `predicted_class` | string | Clasificación ML | "Incluida" o "Excluida" |
| `prediction_confidence` | float | Confianza del modelo ML | 0.0 - 1.0 |
| `sentence_id` | string | ID de oración en documento | "S5678" |
| `sentence_id_paragraph` | string | ID de oración en párrafo | "S1", "S2"... |

#### Columnas Socioeconómicas (solo municipios)

| Columna | Tipo | Descripción | Valores |
|---------|------|-------------|---------|
| `IPM_2018` | float | Índice de Pobreza Multidimensional | 0.0 - 100.0 |
| `PDET` | int | Indicador PDET | 0 o 1 |
| `Cat_IICA` | string | Categoría IICA | "Muy Alto", "Alto", "Medio", "Bajo", "Medio Bajo" |
| `Grupo_MDM` | string | Grupo MDM | "C", "G1", "G2", "G3", "G4", "G5" |

### Proceso de Actualización

#### Opción 1: Reemplazar archivo existente

```bash
# 1. Reemplaza el archivo en Google Drive (mismo FILE_ID)
# 2. En la terminal:
streamlit cache clear
streamlit run app.py
```

#### Opción 2: Nuevo archivo

```bash
# 1. Sube el nuevo archivo a Google Drive
# 2. Obtén el nuevo FILE_ID
# 3. Actualiza .streamlit/secrets.toml:
PARQUET_FILE_ID = "NUEVO_FILE_ID"

# 4. Reinicia el dashboard:
streamlit run app.py
```

### Script de Validación

```python
import pandas as pd
import numpy as np

def validar_parquet(ruta_archivo):
    """Valida estructura del archivo Parquet"""
    
    # Cargar datos
    df = pd.read_parquet(ruta_archivo)
    print(f"✅ Archivo cargado: {len(df):,} registros\n")
    
    # Columnas requeridas
    required_cols = [
        'mpio_cdpmp', 'dpto_cdpmp', 'mpio', 'dpto',
        'tipo_territorio', 'recommendation_code', 'recommendation_text',
        'sentence_similarity', 'sentence_text', 'paragraph_text',
        'paragraph_id', 'page_number'
    ]
    
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        print(f"❌ Columnas faltantes: {missing}\n")
    else:
        print("✅ Todas las columnas requeridas presentes\n")
    
    # Validar tipos de territorio
    print("📊 Tipos de territorio:")
    print(df['tipo_territorio'].value_counts())
    print()
    
    # Validar rangos de similitud
    print("📈 Estadísticas de similitud:")
    print(f"  Mínimo: {df['sentence_similarity'].min():.3f}")
    print(f"  Máximo: {df['sentence_similarity'].max():.3f}")
    print(f"  Promedio: {df['sentence_similarity'].mean():.3f}")
    print()
    
    # Validar códigos DANE
    print("🗺️ Códigos DANE:")
    print(f"  Departamentos únicos: {df['dpto_cdpmp'].nunique()}")
    print(f"  Municipios únicos: {df['mpio_cdpmp'].nunique()}")
    print()
    
    # Validar recomendaciones
    print("📋 Recomendaciones:")
    print(f"  Códigos únicos: {df['recommendation_code'].nunique()}")
    print()
    
    # Detectar valores nulos críticos
    null_counts = df[required_cols].isnull().sum()
    critical_nulls = null_counts[null_counts > 0]
    if len(critical_nulls) > 0:
        print("⚠️ Valores nulos detectados:")
        print(critical_nulls)
    else:
        print("✅ Sin valores nulos en columnas críticas")

# Uso
validar_parquet('tu_archivo.parquet')
```

---

## 🎨 Personalización

### Cambiar Umbral por Defecto

```python
# En vista_general.py, vista_municipal.py, vista_departamental.py
min_similarity = st.sidebar.slider(
    "Similitud Mínima:",
    min_value=0.5,
    max_value=1.0,
    value=0.65,  # ← CAMBIAR AQUÍ (valor por defecto)
    step=0.01
)
```

### Modificar Elementos por Página

```python
# En vista_municipal.py y vista_departamental.py
coincidencias_por_pagina = 5  # ← CAMBIAR AQUÍ (5, 10, 20...)
```

### Ajustar Esquema de Colores del Mapa

```python
# En vista_general.py - función _render_choropleth_map()
fig_map = px.choropleth(
    dept_data,
    color_continuous_scale='viridis',  # ← CAMBIAR AQUÍ
    # Opciones: 'viridis', 'blues', 'reds', 'greens', 'plasma', 'turbo'
)
```

### Personalizar Límite de Memoria DuckDB

```python
# En google_drive_client.py - función conectar_duckdb_parquet()
conn.execute("SET memory_limit='1GB'")  # ← CAMBIAR AQUÍ ('2GB', '4GB'...)
conn.execute("SET threads=2")           # ← CAMBIAR AQUÍ (2, 4, 8...)
```

### Modificar Número de Top Recomendaciones

```python
# En vista_general.py - función _render_implementation_analysis()
top_recs = obtener_top_recomendaciones(
    umbral_similitud=umbral_similitud,
    limite=10  # ← CAMBIAR AQUÍ (5, 10, 15, 20...)
)
```

---

## 🐛 Troubleshooting

### Error: "No se pudo conectar a la base de datos"

**Causa:** `PARQUET_FILE_ID` no configurado o incorrecto.

**Solución:**
```bash
# 1. Verifica .streamlit/secrets.toml
cat .streamlit/secrets.toml

# 2. Confirma que el archivo en Drive sea público/compartido
# 3. Verifica el FILE_ID copiando el enlace completo
```

### Error: "No se encontró el archivo GeoJSON"

**Causa:** Archivo `Mapa Municipios.geojson` no está en la raíz.

**Solución:**
```bash
# Verifica que el archivo exista
ls -la | grep geojson

# Debe mostrar:
# Mapa Municipios.geojson
```

### Mapa no carga geometrías municipales

**Causa:** Códigos DANE no coinciden entre Parquet y GeoJSON.

**Solución:**
```python
# Script de diagnóstico
import pandas as pd
import json

# Cargar datos
df = pd.read_parquet('datos.parquet')
with open('Mapa Municipios.geojson', 'r') as f:
    geojson = json.load(f)

# Verificar códigos departamentales
print("Códigos en Parquet:")
print(df['dpto_cdpmp'].unique()[:5])

print("\nCódigos en GeoJSON:")
dpto_codes = [f['properties']['DPTO_CCDGO'] for f in geojson['features'][:5]]
print(dpto_codes)

# Los códigos deben tener formato:
# Departamentos: "05", "08", "11" (2 dígitos con ceros)
# Municipios: "05001", "08001" (5 dígitos con ceros)
```

**Corrección:**
```python
# Normalizar códigos en el Parquet antes de subirlo
df['dpto_cdpmp'] = df['dpto_cdpmp'].astype(str).str.zfill(2)
df['mpio_cdpmp'] = df['mpio_cdpmp'].astype(str).str.zfill(5)
```

### Caché no se actualiza

**Solución 1: Limpiar caché manualmente**
```bash
streamlit cache clear
```

**Solución 2: Limpiar caché programáticamente**
```python
# En google_drive_client.py, añade parámetro ttl
@st.cache_data(ttl=3600)  # Expira cada hora
def obtener_metadatos_basicos():
    ...
```

### Dashboard muy lento

**Optimizaciones:**

1. **Reducir registros en queries:**
```python
# En google_drive_client.py
query = f"""
    SELECT * FROM datos_principales 
    WHERE {where_clause}
    ORDER BY sentence_similarity DESC
    LIMIT 10000  # ← Añadir límite global
"""
```

2. **Aumentar memoria DuckDB:**
```python
conn.execute("SET memory_limit='2GB'")  # De 1GB a 2GB
```

3. **Reducir precisión de flotantes:**
```python
# Al cargar el Parquet
df['sentence_similarity'] = df['sentence_similarity'].astype('float32')
```

### Error: "ModuleNotFoundError"

**Solución:**
```bash
# Reinstalar dependencias
pip install -r requirements.txt --upgrade

# O instalar módulo específico
pip install nombre-modulo
```

---

## 📊 Modelo de Datos

### Relaciones

```
Territorio (Municipio/Departamento)
  │
  ├── Plan de Desarrollo Territorial (PDT)
  │     │
  │     ├── Página
  │     │   │
  │     │   ├── Párrafo
  │     │   │   │
  │     │   │   └── Oración ──────┐
  │     │   │                      │
  │     │   └── Párrafo            │
  │     │       └── Oración ───────┤
  │     │                          │
  │     └── Página                 │
  │         └── ...                │
  │                                │
  └── Variables Socioeconómicas    │
      (IPM, PDET, IICA, MDM)       │
                                   │
                                   │ Similitud
                                   │ Semántica
Recomendación CEV ←────────────────┘
  ├── Código (MCV1, MJV2, etc.)
  ├── Texto completo
  ├── Tema
  └── Prioridad GN
```

### Queries SQL Dinámicas

El sistema construye queries SQL usando DuckDB:

```sql
-- Ejemplo de query generada dinámicamente
SELECT 
    mpio_cdpmp,
    mpio as Municipio,
    dpto as Departamento,
    COUNT(DISTINCT recommendation_code) as Recomendaciones_Implementadas,
    COUNT(*) as Total_Oraciones,
    AVG(sentence_similarity) as Similitud_Promedio
FROM datos_principales 
WHERE sentence_similarity >= 0.65
AND tipo_territorio = 'Municipio'
AND PDET = 1
AND Cat_IICA IN ('Alto', 'Muy Alto')
AND IPM_2018 BETWEEN 20.0 AND 80.0
AND Grupo_MDM IN ('G3', 'G4')
GROUP BY mpio_cdpmp, mpio, dpto
ORDER BY Recomendaciones_Implementadas DESC
```

---

## 🔒 Seguridad y Buenas Prácticas

### No subir información sensible

```bash
# .gitignore
.streamlit/secrets.toml
*.parquet
*.csv
*.xlsx
__pycache__/
*.pyc
.DS_Store
venv/
.env
```

### Gestión de secrets en producción

Para despliegues en Streamlit Cloud:

1. Ve a **Settings** → **Secrets**
2. Añade:
```toml
PARQUET_FILE_ID = "tu_file_id_aqui"
```

### Límites de recursos

```python
# google_drive_client.py
conn.execute("SET memory_limit='1GB'")      # Límite de RAM
conn.execute("SET threads=2")                # Hilos de CPU
conn.execute("SET temp_directory='/tmp'")   # Archivos temporales
```

---

## 📈 Optimización del Rendimiento

### Caché de Streamlit

```python
# Conexión DuckDB (persiste entre sesiones)
@st.cache_resource
def conectar_duckdb_parquet():
    ...

# Datos (recalcula si cambian inputs)
@st.cache_data
def obtener_metadatos_basicos():
    ...

# Con TTL (expira después de X segundos)
@st.cache_data(ttl=3600)  # 1 hora
def consultar_datos_filtrados():
    ...
```

### Paginación Eficiente

```python
# Malo: Cargar todo y paginar en Python
datos = consultar_datos_filtrados()  # 100,000 registros
pagina_actual = datos.iloc[inicio:fin]

# Bueno: Paginar en SQL
query = f"""
    SELECT * FROM datos_principales
    WHERE ...
    LIMIT {coincidencias_por_pagina}
    OFFSET {(pagina_actual - 1) * coincidencias_por_pagina}
"""
```

### Lazy Loading de Visualizaciones

```python
# Cargar solo cuando el usuario interactúa
with st.expander("Ver mapa municipal", expanded=False):
    if st.session_state.get('cargar_mapa', False):
        _render_municipal_map()
```

## 📞 Contacto y Soporte

### Autor

- **Nombre:** Juan Sebastian Vallejo
- **Email:** js.vallejo08@gmail.com]

### Recursos Adicionales

- **Documentación Streamlit:** https://docs.streamlit.io
- **DuckDB Documentation:** https://duckdb.org/docs/
- **Plotly Express:** https://plotly.com/python/plotly-express/
- **Comisión de la Verdad:** https://www.comisiondelaverdad.co

## 🎓 Créditos

**Desarrollado por:** Juan Sebastian Vallejo
**Versión:** 1.0.0  
**Última actualización:** Octubre 2025  
**Licencia:** MIT

---

<div align="center">

**⭐ Si este proyecto te fue útil, considera darle una estrella en GitHub ⭐**

[🐛 Reportar Bug](https://github.com/tu-usuario/dashboard-cev-colombia/issues) | 
[✨ Solicitar Feature](https://github.com/tu-usuario/dashboard-cev-colombia/issues) | 
[📖 Documentación](https://github.com/tu-usuario/dashboard-cev-colombia/wiki)

---

Hecho con ❤️ en Colombia

</div>
