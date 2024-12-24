# treinta_redshift.
import boto3
import pandas as pd
import time
from io import BytesIO
import datetime
import uuid
from pandas import DataFrame

def assume_role(role_arn, session_name="RedshiftAccessSession"):
    """
    Asume un rol en otra cuenta de AWS y obtiene credenciales temporales.
    
    Parámetros:
    - role_arn: El ARN del rol a asumir.
    - session_name: Nombre de la sesión para las credenciales temporales.
    
    Retorna:
    - Un diccionario con las credenciales temporales (AccessKeyId, SecretAccessKey, SessionToken).
    """
    try:
        sts_client = boto3.client('sts')
        assumed_role = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName=session_name
        )
        return assumed_role['Credentials']
    except Exception as e:
        #print(f"Error al asumir el rol: {e}")
        return None

def table_to_dataframe(table, schema, database='landing_zone', NUM_ENTRIES=0, cluster_identifier='redshift-data', region_name='us-west-2', db_user='admintreinta', credentials= None):
    """
    Ejecuta una consulta SQL en Amazon Redshift para extraer datos de una tabla específica y devuelve los resultados como un DataFrame de pandas.

    Parámetros:
    - table : Nombre de la tabla a consultar.
    - schema: Esquema de la base de datos donde se encuentra la tabla.
    - database: Nombre de la base de datos donde se encuentra la tabla.
    - NUM_ENTRIES: Número máximo de entradas a retornar. Si es 0, retorna todas las entradas.
    - cluster_identifier: Identificador del clúster de Amazon Redshift.
    - db_user: Usuario de la base de datos para ejecutar la consulta.

    Retorna:
    - Un DataFrame de pandas con los resultados de la consulta.
    """
    if credentials:
        client = boto3.client(
        'redshift-data',
        region_name=region_name,
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )
        
    else:
        client = boto3.client('redshift-data', region_name=region_name)
        
    sql_query = f"SELECT * FROM {schema}.{table} "
    if NUM_ENTRIES > 0:
        sql_query += f"LIMIT {NUM_ENTRIES}"

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
        # Espera 5 segundos antes de verificar el estado nuevamente
        time.sleep(0.33)
        status_response = client.describe_statement(Id=statement_id)
        status = status_response['Status']
        #print(f"Current status: {status}")

    if status == 'FINISHED':
        response = client.get_statement_result(Id=statement_id)

        # Extrayendo los nombres de las columnas de la metadata de columnas
        column_metadata = response['ColumnMetadata']
        column_names = [column['name'] for column in column_metadata]

        # Construyendo el DataFrame
        records = response['Records']
        df_rows = []
        for record in records:
            row = []
            for field in record:
                if 'isNull' in field and field['isNull']:
                    row.append(None)
                elif 'stringValue' in field:
                    row.append(field['stringValue'])
                elif 'longValue' in field:
                    row.append(field['longValue'])
                elif 'doubleValue' in field:
                    row.append(field['doubleValue'])
                elif 'booleanValue' in field:
                    row.append(field['booleanValue'])
                else:
                    # Añadir soporte para más tipos según sea necesario
                    row.append(None)
            df_rows.append(row)

        df = DataFrame(df_rows, columns=column_names)

        return df, status
    
    elif status == 'FAILED':
        # Obtiene y muestra el mensaje de error
        error_message = status_response.get(
            'Error', 'No se proporcionó información de error.')
        #print(f"Error: {error_message}")
        return DataFrame(), status  # Retorna un DataFrame vacío si la consulta falla
    else:
        #print("La operación fue abortada o no se completó exitosamente.")
        return DataFrame(), status  # Retorna un DataFrame vacío si la consulta falla


def query_to_dataframe(sql_query, cluster_identifier='redshift-data', database="landing_zone", region_name='us-west-2', db_user='admintreinta', credentials= None):
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
    if credentials:
        client = boto3.client(
        'redshift-data',
        region_name=region_name,
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )
        
    else:
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
        # Espera 5 segundos antes de verificar el estado nuevamente
        time.sleep(0.33)
        status_response = client.describe_statement(Id=statement_id)
        status = status_response['Status']
        #print(f"Current status: {status}")

    if status == 'FINISHED':
        response1 = client.get_statement_result(Id=statement_id)

        # Extrayendo los nombres de las columnas de la metadata de columnas
        column_metadata = response1['ColumnMetadata']
        column_names = [column['name'] for column in column_metadata]

        # Construyendo el DataFrame
        df = DataFrame([[
            field.get('stringValue') if 'stringValue' in field else
            field.get('longValue') if 'longValue' in field else
            field.get('doubleValue') if 'doubleValue' in field else
            field.get('booleanValue') if 'booleanValue' in field else
            None for field in record] for record in response1['Records']],
            columns=column_names)

        return df, status
    elif status == 'FAILED':
        # Obtiene y muestra el mensaje de error
        error_message = status_response.get('Error', 'No se proporcionó información de error.')
        raise Exception(error_message) #Paramos proceso si falla algún flujo
    else:
        raise Exception("La operación fue abortada o no se completó exitosamente.")


def dataframe_to_s3(df, bucket="redshift-python-datalake", endpoint='data_lake', region_name='us-west-2', object_name='', credentials= None):
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

    if credentials:
        s3_resource = boto3.resource(
            's3',
            region_name=region_name,
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken']
        )
    else:
        s3_resource = boto3.resource('s3', region_name=region_name)
        
    s3_resource.Object(bucket, object_path).put(Body=csv_buffer.getvalue())

    return f's3://{bucket}/{object_path}'


def load_s3_to_redshift(table, schema, s3_object_path, database='landing_zone', cluster_identifier='redshift-data', db_user='admintreinta', region_name='us-west-2', credentials= None):
    
    if credentials:
        client = boto3.client(
        'redshift-data',
        region_name=region_name,
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )
        
    else:
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

    #print(sql)
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
        time.sleep(0.33)  # Espera 5 segundos antes de consultar nuevamente
        status_response = client.describe_statement(Id=statement_id)
        status = status_response['Status']
        #print(f"Estado actual: {status}")

    # Verifica el resultado de la ejecución
    if status == 'FINISHED':
        pass
        #print("La carga ha sido exitosa.")
    elif status == 'FAILED':
        # Obtiene y muestra el mensaje de error
        error_message = status_response.get('Error', 'No se proporcionó información de error.')
        raise Exception(error_message)
    else:
        raise Exception("La operación fue abortada o no se completó exitosamente.")

    return status


def execute_SP(store_procedure, schema, database="landing_zone", cluster_identifier='redshift-data', db_user='admintreinta', region_name='us-west-2', credentials= None):
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
    
    if credentials:
        client = boto3.client(
        'redshift-data',
        region_name=region_name,
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )
        
    else:
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
        # Espera 5 segundos antes de verificar el estado nuevamente
        time.sleep(0.33)
        status_response = client.describe_statement(Id=statement_id)
        status = status_response['Status']
        #print(f"Current status: {status}")
    if status == 'FINISHED':
        #print('Store Procedure ejecutado')
        return status
    
    elif status == 'FAILED':
        # Obtiene y muestra el mensaje de error
        error_message = status_response.get('Error', 'No se proporcionó información de error.')
        raise Exception(error_message)
    
    else:
        raise Exception("La operación fue abortada o no se completó exitosamente.")


def truncate_table(table, schema, database="landing_zone", cluster_identifier='redshift-data', db_user='admintreinta', region_name='us-west-2', credentials= None):
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
    if credentials:
        client = boto3.client(
        'redshift-data',
        region_name=region_name,
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )
        
    else:
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
        # Espera 5 segundos antes de verificar el estado nuevamente
        time.sleep(0.33)
        status_response = client.describe_statement(Id=statement_id)
        status = status_response['Status']
        #print(f"Current status: {status}")

    if status == 'FINISHED':
        pass
        #print('Store Procedure ejecutado')
    elif status == 'FAILED':
        # Obtiene y muestra el mensaje de error
        error_message = status_response.get(
            'Error', 'No se proporcionó información de error.')
        #print(f"Error al truncar la tabla: {error_message}")
    else:
        pass
        #print("La operación fue abortada o no se completó exitosamente.")

    return status

def drop_table(table, schema, database="landing_zone", cluster_identifier='redshift-data', db_user='admintreinta', region_name='us-west-2', credentials= None):
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
    if credentials:
        client = boto3.client(
        'redshift-data',
        region_name=region_name,
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )
        
    else:
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
        # Espera 5 segundos antes de verificar el estado nuevamente
        time.sleep(0.33)
        status_response = client.describe_statement(Id=statement_id)
        status = status_response['Status']
        #print(f"Current status: {status}")

    if status == 'FINISHED':
        #print('Taabla eliminada!')
        pass
    elif status == 'FAILED':
        # Obtiene y muestra el mensaje de error
        error_message = status_response.get(
            'Error', 'No se proporcionó información de error.')
        #print(f"Error al eliminar la tabla: {error_message}")
        pass
    else:
        pass
        #print("La operación fue abortada o no se completó exitosamente.")

    return status

def sql_query(sql_query, database="landing_zone", cluster_identifier='redshift-data', db_user='admintreinta', region_name='us-west-2', credentials= None):
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
    if credentials:
        client = boto3.client(
        'redshift-data',
        region_name=region_name,
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )
        
    else:
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
        # Espera 5 segundos antes de verificar el estado nuevamente
        time.sleep(0.33)
        status_response = client.describe_statement(Id=statement_id)
        status = status_response['Status']
        #print(f"Current status: {status}")

    if status == 'FINISHED':
        pass
        #print('Query ejecutada!')

    elif status == 'FAILED':
        # Obtiene y muestra el mensaje de error
        error_message = status_response.get('Error', 'No se proporcionó información de error.')
        raise Exception(error_message)

    else:
        raise Exception("La operación fue abortada o no se completó exitosamente.")

    return status


def dataframe_to_redshift(df, table, schema, bucket="redshift-python-datalake", database='landing_zone', region_name='us-west-2', endpoint='data_lake', object_name=None, db_user='admintreinta', cluster_identifier='redshift-data', credentials= None):
    # Asegúrate de que todos los argumentos se pasen por nombre
    s3_object_path = dataframe_to_s3(df=df, bucket=bucket, endpoint=endpoint, region_name=region_name, object_name=object_name,credentials = credentials)
    output = load_s3_to_redshift(table=table, schema=schema, s3_object_path=s3_object_path, database=database, cluster_identifier=cluster_identifier, db_user=db_user, region_name=region_name, credentials = credentials)
    return output
