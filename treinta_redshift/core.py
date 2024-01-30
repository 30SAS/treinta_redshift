# treinta_redshift
import boto3
import pandas as pd
import time
from io import BytesIO
import datetime
import uuid
import pandas as pd

def table_to_dataframe(table,schema,database='landing_zone', NUM_ENTRIES = 0, cluster_identifier = 'redshift-data', db_user = 'admintreinta', region_name='us-west-2'):
    """
    Ejecuta una consulta SQL en Amazon Redshift y devuelve los resultados como un DataFrame de pandas.
    
    Parámetros:
    - table : Tabla a copiar en un dataframe
    - schema: schema de la tabla a copiar en un datafrmae
    - database: database de la tabla a copiar en un dataframe
    - LIMIT: limitar entries
    - cluster_identifier: Identificador del cluster de Amazon Redshift.
    - database: Nombre de la base de datos.
    - db_user: Usuario de la base de datos.
    
    
    Retorna:
    - Un DataFrame de pandas con los resultados de la consulta.
    """
    client = boto3.client('redshift-data', region_name=region_name)
    sql_query = f"SELECT * FROM {database}.{schema}.{table} "
    if NUM_ENTRIES != 0:
        sql_query = sql_query + f"LIMIT {str(NUM_ENTRIES)}"
        
    response = client.execute_statement(
        ClusterIdentifier=cluster_identifier,
        Database=database,
        DbUser=db_user,
        Sql=sql_query
    )

    statement_id = response['Id']
    
    # Espera hasta que la consulta se haya completado
    status = ''
    while status not in ['FINISHED', 'FAILED', 'ABORTED']:
        time.sleep(5)  # Espera 5 segundos antes de verificar el estado nuevamente
        status_response = client.describe_statement(Id=statement_id)
        status = status_response['Status']
        print(f"Current status: {status}")
    
    if status == 'FINISHED':
        response1 = client.get_statement_result(Id=statement_id)
        
        # Extrayendo los nombres de las columnas de la metadata de columnas
        column_metadata = response1['ColumnMetadata']
        column_names = [column['name'] for column in column_metadata]
        
        # Construyendo el DataFrame
        df = pd.DataFrame([[field.get('stringValue', '') for field in record] for record in response1['Records']], columns=column_names)
        
        return df
    elif status == 'FAILED':
        # Obtiene y muestra el mensaje de error
        error_message = status_response.get('Error', 'No se proporcionó información de error.')
        print(f"Error: {error_message}")
        return pd.DataFrame()  # Retorna un DataFrame vacío si la consulta falla
    else:
        print("La operación fue abortada o no se completó exitosamente.")
        return pd.DataFrame()  # Retorna un DataFrame vacío si la consulta falla


def query_to_dataframe(sql_query, cluster_identifier = 'redshift-data', database = "landing_zone", db_user = 'admintreinta', region_name='us-west-2'):
    """
    Ejecuta una consulta SQL en Amazon Redshift y devuelve los resultados como un DataFrame de pandas.
    
    Parámetros:
    - sql_query: Consulta SQL para ejecutar.
    - cluster_identifier: Identificador del cluster de Amazon Redshift.
    - database: Nombre de la base de datos.
    - db_user: Usuario de la base de datos.
    
    
    Retorna:
    - Un DataFrame de pandas con los resultados de la consulta.
    """
    client = boto3.client('redshift-data', region_name=region_name)
    
    response = client.execute_statement(
        ClusterIdentifier=cluster_identifier,
        Database=database,
        DbUser=db_user,
        Sql=sql_query
    )

    statement_id = response['Id']
    
    # Espera hasta que la consulta se haya completado
    status = ''
    while status not in ['FINISHED', 'FAILED', 'ABORTED']:
        time.sleep(5)  # Espera 5 segundos antes de verificar el estado nuevamente
        status_response = client.describe_statement(Id=statement_id)
        status = status_response['Status']
        print(f"Current status: {status}")
    
    if status == 'FINISHED':
        response1 = client.get_statement_result(Id=statement_id)
        
        # Extrayendo los nombres de las columnas de la metadata de columnas
        column_metadata = response1['ColumnMetadata']
        column_names = [column['name'] for column in column_metadata]
        
        # Construyendo el DataFrame
        df = pd.DataFrame([[field.get('stringValue', '') for field in record] for record in response1['Records']], columns=column_names)
        
        return df
    elif status == 'FAILED':
        # Obtiene y muestra el mensaje de error
        error_message = status_response.get('Error', 'No se proporcionó información de error.')
        print(f"Error: {error_message}")
        return pd.DataFrame()  # Retorna un DataFrame vacío si la consulta falla
    else:
        print("La operación fue abortada o no se completó exitosamente.")
        return pd.DataFrame()  # Retorna un DataFrame vacío si la consulta falla

def dataframe_to_s3(df, bucket="redshift-python-datalake", endpoint='data_lake', object_name=''):
    # Generar un sello de tiempo con el formato deseado
    timestamp = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
    year = datetime.datetime.now().strftime('%Y')
    month = datetime.datetime.now().strftime('%m')
    day = datetime.datetime.now().strftime('%d')
    # Generar un identificador único (puedes reemplazarlo por cualquier otra cadena aleatoria si prefieres)
    if object_name == '':
        object_name = uuid.uuid4().hex
        
    object_path = f"{endpoint}/{year}/{month}/{day}/{object_name}_{timestamp}.csv.gz"

    # Usar BytesIO para datos binarios
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False, compression='gzip')
    
    # Es necesario mover el puntero del buffer al inicio después de escribir en él
    csv_buffer.seek(0)

    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, object_path).put(Body=csv_buffer.getvalue())
    
    return f's3://{bucket}/{object_path}'

def load_s3_to_redshift(table,schema, s3_object_path, database='landing_zone', cluster_identifier='redshift-data', db_user='admintreinta', region_name='us-west-2'):
    client = boto3.client('redshift-data', region_name=region_name)
    sql = f"""
        COPY {database}.{schema}.{table}
        FROM '{s3_object_path}'
        IAM_ROLE default
        delimiter ','
        IGNOREHEADER 1
        GZIP
        CSV;
    """

    print(sql)
    # Ejecuta el comando COPY
    response = client.execute_statement(
        ClusterIdentifier=cluster_identifier,
        Database=database,
        DbUser=db_user,
        Sql=sql
    )
    
    statement_id = response['Id']
    
    # Espera a que la ejecución termine
    status = 'STARTED'
    while status in ['SUBMITTED', 'STARTED', 'PICKED']:
        time.sleep(5)  # Espera 5 segundos antes de consultar nuevamente
        status_response = client.describe_statement(Id=statement_id)
        status = status_response['Status']
        print(f"Estado actual: {status}")

    # Verifica el resultado de la ejecución
    if status == 'FINISHED':
        print("La carga ha sido exitosa.")
    elif status == 'FAILED':
        # Obtiene y muestra el mensaje de error
        error_message = status_response.get('Error', 'No se proporcionó información de error.')
        print(f"Error al truncar la tabla: {error_message}")
    else:
        print("La operación fue abortada o no se completó exitosamente.")
    
        
    return response

def execute_SP(store_procedure,schema,database = "landing_zone", cluster_identifier = 'redshift-data', db_user = 'admintreinta', region_name='us-west-2'):
    """
    Ejecuta una un store en Amazon Redshift
    
    Parámetros:
    - store_procedure: nombre del store procedure a ejecutar
    - schema: Esquema en el que se encuentra el store procedure.
    - database: Nombre de la base de datos.
    - cluster_identifier: Identificador del cluster de Amazon Redshift.
    - db_user: Usuario de la base de datos.
    
    
    Retorna:
    - Un DataFrame de pandas con los resultados de la consulta.
    """
    client = boto3.client('redshift-data', region_name=region_name)
    
    sql_query = f"CALL {database}.{schema}.{store_procedure}()"
    response = client.execute_statement(
        ClusterIdentifier=cluster_identifier,
        Database=database,
        DbUser=db_user,
        Sql=sql_query
    )

    statement_id = response['Id']
    
    # Espera hasta que la consulta se haya completado
    status = ''
    while status not in ['FINISHED', 'FAILED', 'ABORTED']:
        time.sleep(5)  # Espera 5 segundos antes de verificar el estado nuevamente
        status_response = client.describe_statement(Id=statement_id)
        status = status_response['Status']
        print(f"Current status: {status}")
    if status == 'FINISHED':
        print ('Store Procedure ejecutado')
    elif status == 'FAILED':
        # Obtiene y muestra el mensaje de error
        error_message = status_response.get('Error', 'No se proporcionó información de error.')
        print(error_message)
    else:
        print("La operación fue abortada o no se completó exitosamente.")
        return 0

def truncate_table(table, schema, database = "landing_zone", cluster_identifier = 'redshift-data', db_user = 'admintreinta', region_name='us-west-2'):
    """
    Ejecuta una un store en Amazon Redshift
    
    Parámetros:
    - table: tabla a truncar
    - schema: Esquema en el que se encuentra el store procedure.
    - store_procedure: nombre del store procedure a ejecutar
    - cluster_identifier: Identificador del cluster de Amazon Redshift.
    - database: Nombre de la base de datos.
    - db_user: Usuario de la base de datos.
    
    
    Retorna:
    - Un DataFrame de pandas con los resultados de la consulta.
    """
    client = boto3.client('redshift-data', region_name=region_name)
    
    sql_query = f"TRUNCATE {database}.{schema}.{table}"
    response = client.execute_statement(
        ClusterIdentifier=cluster_identifier,
        Database=database,
        DbUser=db_user,
        Sql=sql_query
    )

    statement_id = response['Id']
    
    # Espera hasta que la consulta se haya completado
    status = ''
    while status not in ['FINISHED', 'FAILED', 'ABORTED']:
        time.sleep(5)  # Espera 5 segundos antes de verificar el estado nuevamente
        status_response = client.describe_statement(Id=statement_id)
        status = status_response['Status']
        print(f"Current status: {status}")
    
    if status == 'FINISHED':
        print ('Store Procedure ejecutado')
    elif status == 'FAILED':
        # Obtiene y muestra el mensaje de error
        error_message = status_response.get('Error', 'No se proporcionó información de error.')
        print(f"Error al truncar la tabla: {error_message}")
    else:
        print("La operación fue abortada o no se completó exitosamente.")
    return 0

def drop_table(table, schema, database = "landing_zone", cluster_identifier = 'redshift-data', db_user = 'admintreinta', region_name='us-west-2'):
    """
    Ejecuta una un store en Amazon Redshift
    
    Parámetros:
    - table: tabla a truncar
    - schema: Esquema en el que se encuentra el store procedure.
    - database: Nombre de la base de datos.
    - store_procedure: nombre del store procedure a ejecutar
    - cluster_identifier: Identificador del cluster de Amazon Redshift.
    
    - db_user: Usuario de la base de datos.
    
    
    Retorna:
    - Un DataFrame de pandas con los resultados de la consulta.
    """
    client = boto3.client('redshift-data', region_name=region_name)
    
    sql_query = f"DROP TABLE {database}.{schema}.{table}"
    response = client.execute_statement(
        ClusterIdentifier=cluster_identifier,
        Database=database,
        DbUser=db_user,
        Sql=sql_query
    )

    statement_id = response['Id']
    
    # Espera hasta que la consulta se haya completado
    status = ''
    while status not in ['FINISHED', 'FAILED', 'ABORTED']:
        time.sleep(5)  # Espera 5 segundos antes de verificar el estado nuevamente
        status_response = client.describe_statement(Id=statement_id)
        status = status_response['Status']
        print(f"Current status: {status}")
    
    if status == 'FINISHED':
        print ('Taabla eliminada!')
    elif status == 'FAILED':
        # Obtiene y muestra el mensaje de error
        error_message = status_response.get('Error', 'No se proporcionó información de error.')
        print(f"Error al eliminar la tabla: {error_message}")
    else:
        print("La operación fue abortada o no se completó exitosamente.")
        return 0

def sql_query(sql_query, database = "landing_zone",cluster_identifier = 'redshift-data', db_user = 'admintreinta', region_name='us-west-2'):
    """
    Ejecuta una un store en Amazon Redshift
    
    Parámetros:
    - sql_query: query a ejecutar en SQL
    - database: Nombre de la base de datos.
    - cluster_identifier: Identificador del cluster de Amazon Redshift.
    - db_user: Usuario de la base de datos.
    
    
    Retorna:
    - Un DataFrame de pandas con los resultados de la consulta.
    """
    client = boto3.client('redshift-data', region_name=region_name)
    
    response = client.execute_statement(
        ClusterIdentifier=cluster_identifier,
        Database=database,
        DbUser=db_user,
        Sql=sql_query
    )

    statement_id = response['Id']
    
    # Espera hasta que la consulta se haya completado
    status = ''
    while status not in ['FINISHED', 'FAILED', 'ABORTED']:
        time.sleep(5)  # Espera 5 segundos antes de verificar el estado nuevamente
        status_response = client.describe_statement(Id=statement_id)
        status = status_response['Status']
        print(f"Current status: {status}")
    
    if status == 'FINISHED':
        print ('Query ejecutada!')
    elif status == 'FAILED':
        # Obtiene y muestra el mensaje de error
        error_message = status_response.get('Error', 'No se proporcionó información de error.')
        print(f"Error ejecutando la query SQL: {error_message}")
    else:
        print("La operación fue abortada o no se completó exitosamente.")

        return 0

def dataframe_to_redshift(df,table,schema,bucket = "redshift-python-datalake" ,database='landing_zone',endpoint = 'data_lake',object_name = False,db_user ='admintreinta', cluster_identifier = 'redshift-data'):
    s3_object_path = dataframe_to_s3(df, bucket, endpoint, object_name)
    output = load_s3_to_redshift(table,schema, s3_object_path, database, cluster_identifier, db_user)
    return output