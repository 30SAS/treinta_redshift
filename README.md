# treinta_redshift

`treinta_redshift` es una librería Python diseñada para facilitar la interacción con Amazon Redshift, permitiendo a los usuarios de Python realizar operaciones comunes como consultar datos, cargar datos desde y hacia S3, ejecutar procedimientos almacenados, y manejar tablas dentro de Redshift de una manera sencilla y eficiente.

## Características

- Consultar datos de Redshift y cargar los resultados en un DataFrame de pandas.
- Exportar DataFrames de pandas a S3 en formatos CSV o Parquet.
- Cargar datos desde archivos en S3 a tablas en Redshift.
- Ejecutar procedimientos almacenados en Redshift.
- Truncar y eliminar tablas en Redshift.

## Instalación

Puedes instalar `treinta_redshift` utilizando pip:

```bash
pip install treinta_redshift
```

## Uso

### Consultar Datos de Redshift

```python
from treinta-redshift import query_to_dataframe

sql_query = "SELECT * FROM mi_esquema.mi_tabla LIMIT 10;"
df = query_to_dataframe(sql_query, cluster_identifier='mi-cluster', database='mi-database', db_user='mi-usuario')
print(df)
```

### Cargar Datos a S3

```python
from treinta-redshift import dataframe_to_s3
import pandas as pd

df = pd.DataFrame({'col1': [1, 2], 'col2': ['A', 'B']})
s3_path = dataframe_to_s3(df, bucket='mi-bucket-s3')
print(f"Datos cargados a {s3_path}")
```

### Cargar Datos desde S3 a Redshift

```python
from treinta-redshift import load_s3_to_redshift

load_s3_to_redshift('mi_tabla', 's3://mi-bucket-s3/mi-archivo.csv', cluster_identifier='mi-cluster', database='mi-database', db_user='mi-usuario')
```


## Licencia

Este proyecto está licenciado bajo la Licencia MIT - vea el archivo `LICENSE.md` para más detalles.

## Soporte

Si encuentras un problema o tienes una pregunta, por favor abre un issue en el [repositorio de GitHub](#).

