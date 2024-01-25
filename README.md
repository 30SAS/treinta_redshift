# treinta_redshift

`treinta_redshift` es una poderosa librería de Python diseñada específicamente para ingenieros de datos, analistas de Insights & Analytics, y profesionales de Data Science. Facilita la interacción con Amazon Redshift, permitiendo realizar desde operaciones básicas hasta avanzadas con mínima complejidad y máxima eficiencia.

## Ventajas Clave

- **Simplificación de Operaciones con Redshift**: Simplifica la ejecución de consultas SQL, la manipulación de tablas, y la ejecución de procedimientos almacenados, convirtiendo complejas tareas de gestión de bases de datos en operaciones sencillas y directas.
- **Integración Directa con Pandas**: Permite una integración fluida con pandas, transformando los resultados de consultas SQL directamente en DataFrames y viceversa, lo cual es ideal para análisis de datos y machine learning.
- **Optimización de Cargas de Datos**: Aprovecha la eficiencia del almacenamiento en S3 y la velocidad de la instrucción COPY de Redshift para cargar grandes volúmenes de datos de manera eficiente, utilizando un enfoque de Data Lake.
- **Flexibilidad y Control**: Ofrece a los usuarios control total sobre sus datos, permitiendo operaciones a nivel de base de datos, esquema, o tabla, y facilitando ajustes finos como la limitación del número de entradas a recuperar.

## Requisitos Previos

Para comenzar con `treinta_redshift`, asegúrate de tener lo siguiente:

- Python 3.11 o superior instalado en tu entorno de desarrollo.
- Acceso configurado a AWS, con un rol de AWS IAM que tenga los permisos necesarios para interactuar con los servicios de Redshift y S3.

### Políticas de IAM Requeridas

El rol de IAM debe tener políticas adjuntas que permitan las acciones necesarias en Redshift y S3. Aquí se muestran ejemplos de políticas mínimas requeridas:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "redshift:GetClusterCredentials",
            "Resource": "arn:aws:redshift:tu-región:tu-id-de-cuenta:dbname:tu-nombre-de-base-de-datos/dbuser:tu-usuario-de-base-de-datos"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::tu-bucket-s3/*",
                "arn:aws:s3:::tu-bucket-s3"
            ]
        }
    ]
}
```

Reemplaza `tu-región`, `tu-id-de-cuenta`, `tu-nombre-de-base-de-datos`, `tu-usuario-de-base-de-datos` y `tu-bucket-s3` con tus propios valores.

## Instalación

Instala `treinta_redshift` fácilmente usando pip:

```bash
pip install treinta_redshift
```

Este comando instala la última versión de `treinta_redshift` en tu entorno Python, dejándola lista para ser importada y utilizada en tus proyectos.

Aquí tienes la continuación y conclusión del `README.md` para tu librería `treinta_redshift`:

## Uso de la Librería

`treinta_redshift` incluye varios métodos diseñados para facilitar la interacción con Amazon Redshift. A continuación, se presentan ejemplos de cómo utilizar algunos de los métodos más importantes:

### 1. `table_to_dataframe`

Consulta una tabla en Redshift y convierte los resultados en un DataFrame de pandas. Ideal para análisis de datos.

**Ejemplo:**

```python
df = table_to_dataframe(table='ventas', schema='public', database='mi_database', NUM_ENTRIES=100, cluster_identifier='mi-cluster-redshift', db_user='usuario_redshift')
```
Consulta las primeras 100 entradas de la tabla `ventas`, convirtiendo los resultados en un DataFrame para análisis posterior.

### 2. `query_to_dataframe`

Ejecuta consultas SQL personalizadas en Redshift y devuelve los resultados como DataFrames de pandas. 

**Ejemplo:**

```python
sql_query = "SELECT fecha, SUM(ventas) FROM public.ventas WHERE fecha > '2021-01-01' GROUP BY fecha;"
df = query_to_dataframe(sql_query=sql_query, cluster_identifier='mi-cluster-redshift', database='mi_database', db_user='usuario_redshift')
```
Realiza un análisis agregado de las ventas por fecha.

### 3. `dataframe_to_s3`

Exporta DataFrames de pandas a S3 como archivos CSV comprimidos, preparándolos para su carga en Redshift.

**Ejemplo:**

```python
df = pd.DataFrame({'columna1': [1, 2, 3], 'columna2': ['a', 'b', 'c']})
path_s3 = dataframe_to_s3(df=df, bucket="mi_bucket_s3", endpoint='mi_datalake', object_name='datos_ventas')
```
Guarda `df` como un archivo `.csv.gz` en `mi_bucket_s3`, listo para ser cargado en Redshift.

### 4. `load_s3_to_redshift`

Carga datos desde S3 a Redshift de manera eficiente, utilizando la instrucción COPY de Redshift.

**Ejemplo:**

```python
load_s3_to_redshift(table='ventas', schema='public', s3_object_path='s3://mi_bucket_s3/mi_datalake/mis_datos_ventas.csv.gz', database='mi_database', cluster_identifier='mi-cluster-redshift', db_user='usuario_redshift')
```
Carga datos desde `mis_datos_ventas.csv.gz` en S3 a la tabla `ventas` en Redshift.

### 5. `dataframe_to_redshift`

Combina los pasos de exportación a S3 y carga en Redshift en una sola acción, simplificando el flujo de trabajo.

**Ejemplo:**

```python
df = pd.DataFrame({'columna1': [1, 2, 3], 'columna2': ['a', 'b', 'c']})
dataframe_to_redshift(df=df, table='ventas', schema='public', database='mi_database', db_user='usuario_redshift', cluster_identifier='mi-cluster-redshift')
```
Carga `df` directamente a la tabla `ventas` en Redshift, facilitando un proceso de análisis de datos eficiente y automatizado.

## Carga de Datos a Redshift

Para cargar datos en Redshift, `treinta_redshift` aprovecha un enfoque basado en Data Lake usando S3. Los archivos se exportan primero a S3 en formato `.csv.gz` para luego ser cargados en Redshift con la instrucción COPY. Este proceso está diseñado para ser eficiente y seguro, garantizando una integración de datos rápida y sin problemas.

### Políticas de IAM para Carga de Datos

Asegúrate de que el rol de IAM tenga permisos para ejecutar la instrucción COPY en Redshift y operaciones relevantes en S3. Consulta la sección "Políticas de IAM Requeridas" para más detalles.

## Licencia

Este proyecto está bajo la Licencia MIT. Consulta el archivo `LICENSE` para más detalles.

## Contacto

Para soporte, preguntas o colaboraciones, por favor, abre un issue en el [repositorio de GitHub](#) para que podamos seguir la conversación.