# treinta_redshift

`treinta_redshift` es una poderosa librería de Python diseñada específicamente para ingenieros de datos, analistas de Insights & Analytics, y profesionales de Data Science. Facilita la interacción con Amazon Redshift, permitiendo realizar desde operaciones básicas hasta avanzadas con mínima complejidad y máxima eficiencia.

## Índice

- [Introducción](#introducción)
- [Ventajas Clave](#ventajas-clave)
- [Requisitos Previos](#requisitos-previos)
  - [Políticas de IAM Requeridas](#políticas-de-iam-requeridas)
- [Instalación](#instalación)
- [Uso de la Librería](#uso-de-la-librería)
  - [1. `sql_query`](#1-sql_query)
  - [2. `dataframe_to_redshift`](#2-dataframe_to_redshift)
  - [3. `table_to_dataframe`](#3-table_to_dataframe)
  - [4. `query_to_dataframe`](#4-query_to_dataframe)
  - [5. `execute_SP`](#5-execute_sp)
  - [6. `truncate_table`](#6-truncate_table)
  - [7. `drop_table`](#7-drop_table)
  - [8. `dataframe_to_s3`](#8-dataframe_to_s3)
  - [9. `load_s3_to_redshift`](#9-load_s3_to_redshift)
- [Carga de Datos a Redshift](#carga-de-datos-a-redshift)
  - [Uso de Amazon S3 como Data Lake](#uso-de-amazon-s3-como-data-lake)
  - [Optimización con la Instrucción COPY de Redshift](#optimización-con-la-instrucción-copy-de-redshift)
  - [Conclusiones](#conclusiones)
  - [Políticas de IAM para Carga de Datos](#políticas-de-iam-para-carga-de-datos)
- [Licencia](#licencia)
- [Contacto](#contacto)

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

### 1. `sql_query`

Esta función permite ejecutar una consulta SQL genérica en Amazon Redshift. Es útil para realizar operaciones de base de datos diversas que no están cubiertas por funciones específicas, como actualizaciones, inserciones o cualquier otra operación SQL personalizada.

**Parámetros:**
Consta de los siguientes parámetros:
- **sql_query (str)**:  
  La consulta SQL que se desea ejecutar en Redshift. Debe ser una cadena que contenga una consulta SQL válida.

- **database (str, opcional)**:  
  Nombre de la base de datos en Redshift donde se ejecutará la consulta. Por defecto, se utiliza 'landing_zone'.

- **cluster_identifier (str, opcional)**:  
  Identificador del clúster de Amazon Redshift donde se encuentra la base de datos.

- **db_user (str, opcional)**:  
  Nombre de usuario para acceder a la base de datos en Amazon Redshift.

**Ejemplo:**

```python
sql_command = "UPDATE public.ventas SET cantidad = cantidad + 1 WHERE id_venta = 123"
sql_query(sql_query=sql_command)
```
Este ejemplo ejecuta un comando SQL para actualizar la cantidad en la tabla `ventas` para un registro específico en Redshift. Esta función es versátil y puede manejar una amplia gama de operaciones SQL.

### 2. `dataframe_to_redshift`

Esta función se utiliza para exportar un DataFrame de pandas directamente a una tabla en Amazon Redshift. Es ideal para situaciones donde necesitas transferir datos procesados o generados en Python a Redshift para su almacenamiento o análisis posterior.

**Parámetros:**
Consta de los siguientes parámetros:
- **df (DataFrame)**:  
  El DataFrame de pandas que se desea exportar a Amazon Redshift.

- **table (str)**:  
  Nombre de la tabla en Redshift donde se cargarán los datos.

- **schema (str)**:  
  Esquema en Redshift donde se encuentra la tabla especificada.

- **bucket (str, opcional)**:  
  El nombre del bucket de Amazon S3 utilizado temporalmente para la transferencia de datos. Por defecto, se utiliza 'redshift-python-datalake'.

- **database (str, opcional)**:  
  Nombre de la base de datos en Redshift donde se encuentra la tabla. Por defecto, se utiliza 'landing_zone'.

- **endpoint (str, opcional)**:  
  El endpoint o ruta dentro del bucket donde se almacenará temporalmente el archivo CSV. Esto puede ayudar a organizar los archivos dentro del bucket.

- **object_name (bool/str, opcional)**:  
  Nombre opcional para el archivo en S3. Si no se proporciona o es `False`, se generará un nombre único.

- **db_user (str, opcional)**:  
  Nombre de usuario para acceder a la base de datos en Amazon Redshift.

- **cluster_identifier (str, opcional)**:  
  Identificador del clúster de Amazon Redshift donde se encuentra la base de datos.

**Ejemplo:**

```python
import pandas as pd

# Crear un DataFrame de ejemplo
data = {'col1': [1, 2, 3], 'col2': [4, 5, 6]}
df_example = pd.DataFrame(data)

# Exportar el DataFrame a Redshift
dataframe_to_redshift(df=df_example, table='tabla_destino', schema='public')
```
En este ejemplo, un DataFrame de pandas se exporta a la tabla `tabla_destino` en el esquema `public` de la base de datos predeterminada en Redshift. Primero, el DataFrame se guarda en S3 y luego se carga en Redshift, lo que facilita la transferencia eficiente de grandes conjuntos de datos.

### 3. `table_to_dataframe`

Esta función se utiliza para ejecutar una consulta SQL en Amazon Redshift y devuelve los resultados como un DataFrame de pandas. Es especialmente útil para transferir datos de una tabla específica en Redshift a un DataFrame para su posterior análisis o manipulación en Python.

**Parámetros:**
Consta de los siguientes parámetros:
- **table (str)**:  
  Nombre de la tabla de la que se quieren extraer los datos. Este parámetro especifica la tabla en Amazon Redshift cuyos datos se desean consultar.

- **schema (str)**:  
  Esquema de la base de datos en Redshift donde se encuentra la tabla especificada. El esquema actúa como un 'namespace' que organiza y separa las tablas dentro de la base de datos.

- **database (str, opcional)**:  
  Nombre de la base de datos en Redshift donde se encuentra la tabla. Por defecto, se utiliza 'landing_zone'. Este parámetro permite seleccionar en qué base de datos dentro de Redshift se ejecutará la consulta.

- **NUM_ENTRIES (int, opcional)**:  
  Número máximo de filas a devolver. Si se establece en un número distinto de cero, la consulta incluirá una cláusula 'LIMIT' para restringir el número de filas devueltas. Por defecto, se obtienen todas las filas de la tabla.

- **cluster_identifier (str, opcional)**:  
  Identificador del clúster de Amazon Redshift. Este parámetro es necesario para conectar con el clúster de Redshift específico donde se encuentra la base de datos.

- **db_user (str, opcional)**:  
  Nombre de usuario para acceder a la base de datos de Amazon Redshift. Este parámetro es utilizado para la autenticación y acceso a la base de datos.


**Ejemplo:**

```python
df = table_to_dataframe(table='products', schema='app', database='cleaning_zone', NUM_ENTRIES=100)
```
Consulta las primeras 100 entradas de la tabla `cleaning_zone.app.products`, convirtiendo los resultados en un DataFrame para análisis posterior.


### 4. `query_to_dataframe`

Esta función se utiliza para ejecutar una consulta SQL específica en Amazon Redshift y devuelve los resultados como un DataFrame de pandas. Es ideal para situaciones donde necesitas realizar consultas personalizadas en lugar de extraer toda una tabla.

**Parámetros:**
Consta de los siguientes parámetros:
- **sql_query (str)**:  
  La consulta SQL que se desea ejecutar en Amazon Redshift. Debe ser una cadena que contenga una consulta SQL válida.

- **cluster_identifier (str, opcional)**:  
  Identificador del clúster de Amazon Redshift donde se ejecutará la consulta. Esencial para establecer la conexión con el clúster específico.

- **database (str, opcional)**:  
  Nombre de la base de datos en Redshift donde se ejecutará la consulta. Por defecto, se utiliza 'landing_zone'. Este parámetro permite especificar la base de datos de destino.

- **db_user (str, opcional)**:  
  Nombre de usuario para acceder a la base de datos de Amazon Redshift. Este parámetro se utiliza para autenticar el acceso a la base de datos.

**Ejemplo:**

```python
sql_query = "SELECT * FROM app.sales WHERE date >= '2024-01-01'"
df_sales = query_to_dataframe(sql_query=sql_query, database='sales_data')
```
Este ejemplo ejecuta una consulta SQL que selecciona todos los registros de ventas desde el 1 de enero de 2024 en la base de datos `sales_data`, convirtiendo los resultados en un DataFrame para un análisis más detallado.

Continúa con la siguiente función cuando estés listo.

### 5. `execute_SP`

Esta función se utiliza para ejecutar un procedimiento almacenado (Stored Procedure) en Amazon Redshift. Es útil para automatizar y ejecutar tareas complejas de bases de datos que están encapsuladas en procedimientos almacenados dentro de Redshift.

**Parámetros:**
Consta de los siguientes parámetros:
- **store_procedure (str)**:  
  Nombre del procedimiento almacenado que se desea ejecutar en Redshift.

- **schema (str)**:  
  Esquema en Redshift donde se encuentra el procedimiento almacenado.

- **database (str, opcional)**:  
  Nombre de la base de datos en Redshift donde se ejecutará el procedimiento almacenado. Por defecto, se utiliza 'landing_zone'.

- **cluster_identifier (str, opcional)**:  
  Identificador del clúster de Amazon Redshift donde se encuentra la base de datos.

- **db_user (str, opcional)**:  
  Nombre de usuario para acceder a la base de datos en Amazon Redshift.

**Ejemplo:**

```python
execute_SP(store_procedure='actualizar_datos_ventas', schema='public')
```
Este ejemplo ejecuta el procedimiento almacenado `actualizar_datos_ventas` en el esquema `public` de la base de datos predeterminada en Redshift. El procedimiento podría, por ejemplo, realizar una serie de transformaciones de datos y actualizaciones de tablas relacionadas con las ventas.

### 6. `truncate_table`

Esta función se utiliza para vaciar todos los registros de una tabla específica en Amazon Redshift, manteniendo su estructura. Es especialmente útil para situaciones en las que necesitas limpiar los datos de una tabla sin eliminar la tabla misma, preparándola para una nueva carga de datos.

**Parámetros:**
Consta de los siguientes parámetros:
- **table (str)**:  
  Nombre de la tabla en Redshift que se desea truncar (vaciar).

- **schema (str)**:  
  Esquema en Redshift donde se encuentra la tabla especificada.

- **database (str, opcional)**:  
  Nombre de la base de datos en Redshift donde se encuentra la tabla. Por defecto, se utiliza 'landing_zone'.

- **cluster_identifier (str, opcional)**:  
  Identificador del clúster de Amazon Redshift donde se encuentra la base de datos.

- **db_user (str, opcional)**:  
  Nombre de usuario para acceder a la base de datos en Amazon Redshift.

**Ejemplo:**

```python
truncate_table(table='logs_temporales', schema='public')
```
Este ejemplo vaciará todos los datos de la tabla `logs_temporales` en el esquema `public` de la base de datos predeterminada en Redshift. Esta operación es útil para eliminar datos obsoletos o temporales de una tabla sin tener que eliminar y recrear la tabla.

### 7. `drop_table`

Esta función se utiliza para eliminar completamente una tabla específica de una base de datos en Amazon Redshift. Es ideal para situaciones donde una tabla ya no es necesaria o debe ser eliminada antes de crear una nueva versión de la misma.

**Parámetros:**
Consta de los siguientes parámetros:
- **table (str)**:  
  Nombre de la tabla en Redshift que se desea eliminar.

- **schema (str)**:  
  Esquema en Redshift donde se encuentra la tabla especificada.

- **database (str, opcional)**:  
  Nombre de la base de datos en Redshift donde se encuentra la tabla. Por defecto, se utiliza 'landing_zone'.

- **cluster_identifier (str, opcional)**:  
  Identificador del clúster de Amazon Redshift donde se encuentra la base de datos.

- **db_user (str, opcional)**:  
  Nombre de usuario para acceder a la base de datos en Amazon Redshift.

**Ejemplo:**

```python
drop_table(table='tabla_antigua', schema='public')
```
Este ejemplo elimina la tabla `tabla_antigua` del esquema `public` en Redshift. Esta acción es irreversible y debe usarse con precaución, ya que todos los datos en la tabla serán eliminados permanentemente.


### 8. `dataframe_to_s3`

Esta función se utiliza para guardar un DataFrame de pandas como un archivo CSV comprimido en un bucket de Amazon S3. Es útil para situaciones donde necesitas exportar datos procesados o analizados desde Python a S3 para almacenamiento o uso posterior en otros servicios de AWS.

**Parámetros:**
Consta de los siguientes parámetros:
- **df (DataFrame)**:  
  El DataFrame de pandas que se desea exportar a Amazon S3.

- **bucket (str, opcional)**:  
  El nombre del bucket de Amazon S3 donde se guardará el archivo. Por defecto, se utiliza 'redshift-python-datalake'.

- **endpoint (str, opcional)**:  
  El endpoint o ruta dentro del bucket donde se guardará el archivo. Esto puede ayudar a organizar los archivos dentro del bucket.

- **object_name (str, opcional)**:  
  Nombre opcional para el archivo en S3. Si no se proporciona, se generará un nombre único basado en un UUID.

**Ejemplo:**

```python
import pandas as pd

# Crear un DataFrame de ejemplo
data = {'col1': [1, 2, 3], 'col2': [4, 5, 6]}
df_example = pd.DataFrame(data)

# Guardar el DataFrame en S3
s3_path = dataframe_to_s3(df=df_example, bucket='mi-bucket-de-datos', endpoint='datos-ejemplo')
```
En este ejemplo, se crea un DataFrame simple y luego se exporta a un archivo CSV comprimido dentro del bucket 'mi-bucket-de-datos' en Amazon S3, bajo la ruta 'datos-ejemplo'. El archivo en S3 tendrá un nombre generado automáticamente.

### 9. `load_s3_to_redshift`

Esta función permite cargar datos desde un archivo en Amazon S3 a una tabla específica en Amazon Redshift. Es ideal para situaciones en las que necesitas importar datos almacenados en S3 a Redshift para su análisis o procesamiento adicional.

**Parámetros:**
Consta de los siguientes parámetros:
- **table (str)**:  
  Nombre de la tabla en Redshift donde se cargarán los datos.

- **schema (str)**:  
  Esquema en Redshift donde se encuentra la tabla especificada.

- **s3_object_path (str)**:  
  Ruta completa del archivo en el bucket de S3 que se desea cargar en Redshift.

- **database (str, opcional)**:  
  Nombre de la base de datos en Redshift donde se encuentra la tabla. Por defecto, se utiliza 'landing_zone'.

- **cluster_identifier (str, opcional)**:  
  Identificador del clúster de Amazon Redshift donde se encuentra la base de datos.

- **db_user (str, opcional)**:  
  Nombre de usuario para acceder a la base de datos en Amazon Redshift.

**Ejemplo:**

```python
s3_object_path = 's3://mi-bucket-de-datos/datos-ejemplo/data.csv.gz'
load_s3_to_redshift(table='ventas', schema='public', s3_object_path=s3_object_path, database='sales_data')
```
En este ejemplo, se carga un archivo CSV comprimido desde el path `s3://mi-bucket-de-datos/datos-ejemplo/data.csv.gz` en S3 a la tabla `ventas` en el esquema `public` de la base de datos `sales_data` en Redshift. Este proceso es crucial para mantener actualizados los datos en Redshift con los archivos almacenados en S3.

## Carga de Datos a Redshift

La eficiencia en la manipulación y carga de grandes volúmenes de datos es crucial en el mundo del Big Data. `treinta_redshift` implementa un enfoque estratégico que combina la robustez de Amazon S3 y la potencia de la instrucción COPY de Amazon Redshift para lograr una carga de datos óptima.

### Uso de Amazon S3 como Data Lake

Amazon S3 actúa como un Data Lake centralizado, almacenando grandes cantidades de datos en un formato estructurado, semi-estructurado y no estructurado. Al exportar los DataFrames de pandas como archivos `.csv.gz` a S3, `treinta_redshift` utiliza la capacidad de almacenamiento y escalabilidad de S3 para manejar datos de manera efectiva. Los beneficios de este enfoque incluyen:

- **Almacenamiento a Largo Plazo y Escalabilidad**: S3 proporciona un almacenamiento seguro y escalable, permitiendo manejar cantidades masivas de datos sin comprometer el rendimiento.
- **Accesibilidad y Seguridad**: Los datos en S3 son accesibles desde cualquier lugar, garantizando al mismo tiempo la seguridad y la integridad de los datos mediante políticas de IAM y otras configuraciones de seguridad.

### Optimización con la Instrucción COPY de Redshift

Una vez que los datos están almacenados en S3, `treinta_redshift` utiliza la instrucción COPY de Redshift para cargarlos en la base de datos. La instrucción COPY es una de las formas más eficientes de cargar grandes conjuntos de datos en Redshift por varias razones:

- **Carga Paralela y Eficiente**: COPY ejecuta una carga de datos paralela, aprovechando la arquitectura distribuida de Redshift para cargar datos de manera rápida y eficiente.
- **Compresión de Datos**: Al trabajar con archivos `.csv.gz`, la instrucción COPY aprovecha la compresión de datos para reducir el tiempo de transferencia y el costo de almacenamiento.
- **Menor Carga Administrativa**: COPY maneja automáticamente el particionado de datos y la asignación de recursos, reduciendo significativamente la carga administrativa y los errores humanos.
- **Optimización del Rendimiento**: Utilizar COPY para la carga de datos permite a Redshift optimizar el uso de recursos y mejorar el rendimiento general de las consultas.

### Conclusiones

La combinación de almacenamiento en S3 y la instrucción COPY de Redshift representa una solución integral para la gestión de datos en `treinta_redshift`. Este enfoque no solo garantiza una carga de datos eficiente y rápida sino que también asegura la escalabilidad, seguridad y accesibilidad de tus datos, facilitando así una gestión de datos más eficaz y menos propensa a errores.

### Políticas de IAM para Carga de Datos

Asegúrate de que el rol de IAM tenga permisos para ejecutar la instrucción COPY en Redshift y operaciones relevantes en S3. Consulta la sección "Políticas de IAM Requeridas" para más detalles.

## Licencia

Este proyecto está bajo la Licencia MIT. Consulta el archivo `LICENSE` para más detalles.

## Contacto

Para soporte, preguntas o colaboraciones, por favor, abre un issue en el [repositorio de GitHub](#) para que podamos seguir la conversación.