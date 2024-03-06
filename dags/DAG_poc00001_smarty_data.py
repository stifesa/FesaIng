# ALNETAHU - 2024

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
import json
import os
import io
import datetime
import pandas_gbq
import numpy as np


#######################################################################################
# PARAMETROS
#######################################################################################
nameDAG           = 'Carga_Smarty_Data'
project           = 'ferreyros-mvp'
owner             = 'ALNETAHU'
email             = ['astroboticapps@gmail.com']
GBQ_CONNECTION_ID = 'bigquery_default'
service_account_path = 'gs://st_raw/crdfesa/ferreyros-mvp-3cf04ce5fdcc.json'
#######################################################################################

def process_and_load_data(**kwargs):
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
        scopes=['https://www.googleapis.com/auth/drive.readonly']
    )

    # Construye el servicio de Google Drive
    service = build('drive', 'v3', credentials=credentials)

     # ID de la carpeta específica en Google Drive
    folder_id = '1afXts2VK7QP9kuw4S66-WFnWJHXegcs8'

    # Query para buscar archivos CSV dentro de la carpeta especificada
    query = f"'{folder_id}' in parents and name contains 'KIT_CONTENIDO' and mimeType='text/csv' and trashed=false"

    # Realiza la búsqueda en Google Drive
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])

    if not items:
        print('No files found.')
    else:
        for item in items:
            print(u'Found file: {0} ({1})'.format(item['name'], item['id']))
            request = service.files().get_media(fileId=item['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                print("Download %d%%." % int(status.progress() * 100))
            # Subir el archivo a Google Cloud Storage
            nuevo_nombre = 'kit_contenido.csv'
            gcs_bucket_name = 'st_raw'
            gcs_blob_name = 'st_raw/smarty_' + nuevo_nombre
            gcs_bucket = storage_client.bucket(gcs_bucket_name)
            gcs_blob = gcs_bucket.blob(gcs_blob_name)
            gcs_blob.upload_from_string(fh.getvalue(), content_type='text/csv')
            print(f"Archivo {nuevo_nombre} subido a GCS en {gcs_blob_name}.")

    # Aquí puedes realizar las transformaciones necesarias en el DataFrame
    # Por ejemplo: df = df.transform(...)

def load_csv_to_bigquery(**kwargs):
    
    # Crea un cliente de GCS
    client = storage.Client()

    # Información de BigQuery
    dataset = 'raw_st'
    tabla0001 = 'pre_contenido'
    
    # Carga las credenciales y crea un cliente de BigQuery
    bucket_name, blob_name = service_account_path.replace('gs://', '').split('/', 1)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    service_account_content = blob.download_as_bytes()
    print(service_account_content)
    # Carga las credenciales de la cuenta de servicio
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(service_account_content.decode('utf-8')),
        scopes=['https://www.googleapis.com/auth/drive.readonly',
        "https://www.googleapis.com/auth/cloud-platform"]
    )
    client = bigquery.Client(credentials=credentials, project=project)

    # Define el nombre del archivo en GCS y el path local para guardar el archivo
    OBJECT_NAME = 'st_raw/smarty_kit_contenido.csv'
    CSV_PATH = f'gs://{bucket_name}/{OBJECT_NAME}'
    DATASET_NAME = 'raw_st'
    TABLE_NAME = 'pre_contenido'
    table_id = f"{project}.{DATASET_NAME}.{TABLE_NAME}"

    # Lee el archivo CSV en un DataFrame de pandas
    pre_contenido = pd.read_csv(CSV_PATH)
    print(pre_contenido.head(3))
     # Carga el archivo CSV desde GCS a BigQuery
    load_job = client.load_table_from_uri(CSV_PATH, table_id, job_config=job_config)
    load_job.result()  # Espera a que la carga termine
    
default_args = {
    'owner': owner,                   # The owner of the task.
    'depends_on_past': False,         # Task instance should not rely on the previous task's schedule to succeed.
    'start_date': datetime.datetime(2022, 11, 5),
    'email': email,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,  # Retry once before failing the task.
    'retry_delay': datetime.timedelta(minutes=1),  # Time between retries
    'project_id': project,  # Cloud Composer project ID
}

with DAG(nameDAG,
         default_args = default_args,
         catchup = False,  # Ver caso catchup = True
         max_active_runs = 3,
         schedule_interval = "0 6 * * *") as dag: # schedule_interval = None # Caso sin trigger automático | schedule_interval = "0 12 * * *" | "0,2 12 * * *"

    # FUENTE: CRONTRAB: https://crontab.guru/
    #############################################################
    
    t_begin = DummyOperator(task_id="begin")
    
    task_python = PythonOperator(task_id='task_python',
                                 provide_context=True,
                                 python_callable=process_and_load_data
                                 )

    task_bq = PythonOperator(task_id='task_bq',
                                 provide_context=True,
                                 python_callable=load_csv_to_bigquery
                                 )

    t_end = DummyOperator(task_id="end")

    #############################################################
    t_begin >> task_python >> task_bq >> t_end

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
