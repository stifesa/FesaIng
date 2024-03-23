# ALNETAHU - 2024

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import datetime as dt
from datetime import datetime, timedelta
import pandas as pd
import json
import os
import io
import gspread
import logging
from google.cloud import bigquery
from google.cloud import storage
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
#from firebase_admin import credentials, firestore
from google.cloud import firestore
import pandas_gbq
import numpy as np


#######################################################################################
# PARAMETROS
#######################################################################################
nameDAG           = 'Carga_Fire_Data'
project           = 'ferreyros-mvp'
owner             = 'ALNETAHU'
email             = ['astroboticapps@gmail.com']
GBQ_CONNECTION_ID = 'bigquery_default'
service_account_path = 'gs://st_raw/crdfesa/ferreyros-mvp-3cf04ce5fdcc.json'
service_account_fire =  'gs://st_raw/crdfesa/ferreyros-mvp-9fc91ce58466.json'

#######################################################################################

def dsp_load_data(**kwargs):
    # Configura el path donde tu archivo CSV será almacenado
    storage_client = storage.Client()
    bucket_name, blob_name = service_account_fire.replace('gs://', '').split('/', 1)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    service_account_content = blob.download_as_bytes()
    print(service_account_content)
    # Carga las credenciales de la cuenta de servicio
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(service_account_content.decode('utf-8')),
        scopes=['https://www.googleapis.com/auth/drive.readonly',
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/datastore"]
    )

    bucket_name2, blob_name2 = service_account_path.replace('gs://', '').split('/', 1)
    bucket = storage_client.bucket(bucket_name2)
    blob = bucket.blob(blob_name2)
    service_account_content2 = blob.download_as_bytes()
    # Carga las credenciales de la cuenta de servicio
    credentials2 = service_account.Credentials.from_service_account_info(
        json.loads(service_account_content2.decode('utf-8')),
        scopes=['https://www.googleapis.com/auth/drive.readonly',
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/datastore"]
    )

    client = bigquery.Client(credentials=credentials2, project=project)

    # Construye el servicio de Google Drive
    db = firestore.Client(credentials=credentials)
    collection_path = 'db/ferreyros/quality'
    docs = db.collection(collection_path).stream()
    data = []
    for doc in docs:
        json_data = doc.to_dict()
        # Aquí puedes agregar el procesamiento de tus datos como se mostró en ejemplos anteriores
        # Por ejemplo, extraer ciertos campos del documento:
        record = {
            'id': json_data.get('id', ''),
            'correctiveActions': json_data.get('correctiveActions', ''),
            # Agrega más campos según sea necesario
        }
        data.append(record)
    
    df = pd.DataFrame(data)
    print(df.head())
    #sqltrunc = """
    #TRUNCATE TABLE `ferreyros-mvp.raw_st.dsp_calidad`
    #"""
    #client.query(sqltrunc)
    # Carga el archivo CSV desde GCS a BigQuery
    DATASET_NAME = 'raw_st'
    TABLE_NAME = 'dsp_acciones'
    table_id = f"{project}.{DATASET_NAME}.{TABLE_NAME}"
    load_job = client.load_table_from_dataframe(df, table_id)
    load_job.result()

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
         schedule_interval = "0 */8 * * *") as dag: # schedule_interval = None # Caso sin trigger automático | schedule_interval = "0 12 * * *" | "0,2 12 * * *"

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
