# ALNETAHU - 2024

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, explode, col
from pyspark.sql.types import StructType, StructField, StringType, LongType
import datetime as dt
from datetime import datetime, timedelta
import pandas as pd
import json
import os
import io
import logging
from google.cloud import bigquery
from google.cloud import storage
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
import pandas_gbq
import numpy as np


#######################################################################################
# PARAMETROS
#######################################################################################
nameDAG           = 'Carga_DSP_Data'
project           = 'ferreyros-mvp'
owner             = 'ALNETAHU'
email             = ['astroboticapps@gmail.com']
GBQ_CONNECTION_ID = 'bigquery_default'
service_account_path = 'gs://st_raw/crdfesa/ferreyros-mvp-3cf04ce5fdcc.json'
spark = SparkSession.builder \
    .appName("Json Parse") \
    .getOrCreate()
#######################################################################################

def dsp_load_data(**kwargs):
    # Configura el path donde tu archivo CSV será almacenado
    storage_client = storage.Client()
    bucket_name, blob_name = service_account_path.replace('gs://', '').split('/', 1)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    service_account_content = blob.download_as_bytes()
    print(service_account_content)
    # Carga las credenciales de la cuenta de servicio
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(service_account_content.decode('utf-8')),
        scopes=['https://www.googleapis.com/auth/drive.readonly',
        "https://www.googleapis.com/auth/cloud-platform"]
    )

    # Construye el servicio de Google Drive
    client = bigquery.Client(credentials=credentials, project=project)

    # Ejecuta la consulta y convierte el resultado en un DataFrame
    sql = """
        WITh deletes AS (SELECT document_id
          FROM `ferreyros-mvp.DataFirestorePrueba.Calidad_raw_changelog`
          WHERE operation = 'DELETE')
        SELECT * FROM `ferreyros-mvp.DataFirestorePrueba.Calidad_raw_changelog` AS A
        LEFT JOIN deletes AS D
        ON A.document_id = D.document_id
        WHERE D.document_id IS NULL
        ORDER BY A.document_id,data,timestamp """
    analysis_schema = StructType([
        StructField('bahia', StringType(), True),
        StructField('basicCause', StringType(), True),
        StructField('observation', StringType(), True),
        StructField('process', StringType(), True),
        StructField('responsable', StringType(), True),
        StructField('causeFailure', StringType(), True),
        StructField('responsibleWorkshop', StructType([
            StructField('workshopName', LongType(), True)
        ]), True)
    ])
    json_schema = StructType([
        StructField('analysis', analysis_schema, True),
        StructField('workOrder', StringType(), True),
        StructField('id', StringType(), True),
        StructField('eventType', StringType(), True),
        StructField('state', StringType(), True),
        StructField('workShop', StringType(), True),
        StructField('miningOperation', StringType(), True),
        StructField('packageNumber', StringType(), True),
        StructField('question1', StringType(), True),
        StructField('packageNumber', StringType(), True),
        StructField('component', StringType(), True),
        StructField('partNumber', StringType(), True),
        StructField('generalImages', StringType(), True),
        StructField('correctiveActions', StringType(), True),
        StructField('createdAt', StructType([
            StructField('_seconds', LongType(), True)
        ]), True),
        StructField('processAt', StructType([
            StructField('_seconds', LongType(), True)
        ]), True),
        StructField('finalizedAt', StructType([
            StructField('_seconds', LongType(), True)
        ]), True),
        StructField('tracingAt', StructType([
            StructField('_seconds', LongType(), True)
        ]), True),
        StructField('reportingWorkshop', StructType([
            StructField('workshopName', LongType(), True)
        ]), True),
        StructField('createdBy', StructType([
            StructField('email', LongType(), True)
        ]), True),
        StructField('specialist', StructType([
            StructField('name', LongType(), True)
        ]), True)
    ])
    def parse_json(x):
        json_data = json.loads(x)
        def get_nested_value(d, key, subkey):
            # Verificar si la clave principal existe y si tiene un subcampo
            if key in d and isinstance(d[key], dict):
                return d[key].get(subkey, '')
            else:
                return ''
    
        # Extraer los campos deseados del JSON
        data = {
            'workOrder': json_data.get('workOrder', ''),
            'id': json_data.get('id', ''),
            'eventType': json_data.get('eventType', ''),
            'state': json_data.get('state', ''),
            'workShop': json_data.get('workShop', ''),
            'miningOperation': json_data.get('miningOperation', ''),
            'packageNumber': json_data.get('packageNumber', ''),
            'question1': json_data.get('question1', ''),
            'component': json_data.get('component', ''),
            'partNumber': json_data.get('partNumber', ''),
            'generalImages': json_data.get('generalImages', ''),
            'correctiveActions': json_data.get('correctiveActions', ''),
            'processAt': json_data.get('processAt', {}).get('_seconds', ''),
            'finalizedAt': json_data.get('finalizedAt', {}).get('_seconds', ''),
            'tracingAt': json_data.get('tracingAt', {}).get('_seconds', ''),
            'createdBy': json_data.get('createdBy', {}).get('email', ''),
            'specialist': json_data.get('specialist', ''),
            'reportingWorkshop': get_nested_value(json_data, 'reportingWorkshop', 'workshopName'),
            'specialist': get_nested_value(json_data, 'specialist', 'name'),
            'createdAt': json_data.get('createdAt', {}).get('_seconds', '')
        }
        return pd.Series(data)
    
    df = client.query(sql).to_dataframe()
    # Realiza transformaciones en el DataFrame
    df = df.reset_index(drop=True)
    df['index'] = np.arange(0, len(df))+1
    #df['json_data'] = '{"' + df['index'].astype(str) + '":'+ df['data'] + "}"
    df['json_data'] = df['data']
    df_parsed = df['json_data'].apply(parse_json)
    print(df_parsed.head())
    #parsed_df = pd.concat(df['json_data'].apply(parse_json).tolist(), ignore_index=True)
    #parsed_df = parsed_df.reset_index(drop=True)
    df_final = pd.concat([df, df_parsed], axis=1)
    df_final.sort_values(['timestamp', 'id'], ascending=[False, True], inplace=True)
    #parsed_df['finalizedAt'] = parsed_df['finalizedAt'].fillna(0)
    #parsed_df['tracingAt'] = parsed_df['tracingAt'].fillna(0)
    #parsed_df['processAt'] = parsed_df['processAt'].fillna(0)
    #parsed_df['creacion'] = pd.to_datetime(parsed_df['createdAt'], unit='s')
    #parsed_df['creacion'] = parsed_df['creacion'].dt.strftime("%d/%m/%Y %H:%M:%S")
    #parsed_df['finalizacion'] = pd.to_datetime(parsed_df['finalizedAt._seconds'], unit='s')
    #parsed_df['finalizacion'] = parsed_df['finalizacion'].dt.strftime("%d/%m/%Y %H:%M:%S")
    #parsed_df['proceso_inicio'] = pd.to_datetime(parsed_df['processAt._seconds'], unit='s')
    #parsed_df['proceso_inicio'] = parsed_df['proceso_inicio'].dt.strftime("%d/%m/%Y %H:%M:%S")
    #parsed_df['seguimiento'] = pd.to_datetime(parsed_df['tracingAt._seconds'], unit='s')
    #parsed_df['seguimiento'] = parsed_df['seguimiento'].dt.strftime("%d/%m/%Y %H:%M:%S")
    parsed_df['Rank'] = 1
    parsed_df['Rank'] = parsed_df.groupby(['id'])['Rank'].cumsum()
    #n_by_iddata = parsed_df.loc[(parsed_df.Rank == 1)]
    #quality=n_by_iddata[['id','eventType','state','timestamp','creacion','finalizacion','seguimiento','index','state','workOrder','workShop','partNumber','generalImages','miningOperation','specialist.name','enventDetail','analysis.observation','analysis.process','analysis.bahia','analysis.basicCause','analysis.responsable','analysis.causeFailure','analysis.responsibleWorkshop.workshopName','reportingWorkshop.workshopName','createdBy.email','correctiveActions','component','proceso_inicio','question1','packageNumber']]
    #quality.columns = ['id','TipoEvento','estado_final','Ultima_mod', 'Fecha_creacion','Fecha_fin','fecha_seguimiento', 'indice','estado','workorder','Taller','NumParte','Imagen','OperacionMin','Especialista','DetalleEvento','Observacion','Proceso','Bahia','CausaBasica','Responsable','CausaFalla','TallerResponable','TallerReporta','email_registro','AccionCorrectiva','component','proceso_inicio','question1','plaqueteo']

    # Define el nombre del archivo en GCS y el path local para guardar el archivo
    DATASET_NAME = 'raw_st'
    TABLE_NAME = 'dsp_calidad'
    table_id = f"{project}.{DATASET_NAME}.{TABLE_NAME}"
    #quality.reset_index(inplace=True, drop=True)
    #quality = quality.astype(str)

    # Carga el archivo CSV desde GCS a BigQuery
    #load_job = client.load_table_from_dataframe(quality, table_id)
    #load_job.result()

default_args = {
    'owner': owner,                   # The owner of the task.
    'depends_on_past': False,         # Task instance should not rely on the previous task's schedule to succeed.
    'start_date': dt.datetime(2022, 11, 5),
    'email': email,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,  # Retry once before failing the task.
    'retry_delay': dt.timedelta(minutes=1),  # Time between retries
    'project_id': project,  # Cloud Composer project ID
}

with DAG(nameDAG,
         default_args = default_args,
         catchup = False,  # Ver caso catchup = True
         max_active_runs = 3,
         schedule_interval = "0 10 * * *") as dag: # schedule_interval = None # Caso sin trigger automático | schedule_interval = "0 12 * * *" | "0,2 12 * * *"

    # FUENTE: CRONTRAB: https://crontab.guru/
    #############################################################
    
    t_begin = DummyOperator(task_id="begin")
    
    task_python = PythonOperator(task_id='task_python',
                                 provide_context=True,
                                 python_callable=dsp_load_data
                                 )

    t_end = DummyOperator(task_id="end")

    #############################################################
    t_begin >> task_python >> t_end

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

### SET PROJECT
#       gcloud config set project txd-capacityplanning-tst

### Ejecuta  fechas NO ejecutadas anteriormente (Tiene que tener schedule_interval)
#       gcloud composer environments run capacity-planning-composer-1 --location us-central1 backfill -- -s 20201101 -e 20201105 DAG-poc01-python-funct
#       -s: start date -> INTERVALO CERRADO
#       -e: end date   -> INTERVALO ABIERTO

### RE-ejecuta fechas anteriores
#       gcloud composer environments run capacity-planning-composer-1 --location us-central1 clear -- -c -s 20201106 -e 20201108 DAG-poc01-python-funct02

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
