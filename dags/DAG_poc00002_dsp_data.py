# ALNETAHU - 2024

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
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
import datetime
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
        ORDER BY A.document_id,data,timestamp limit 20"""
    def parse_json(x):
        return pd.json_normalize(json.loads(x))
    
    df = client.query(sql).to_dataframe()
    print(df.head(2))
    # Realiza transformaciones en el DataFrame
    df = df.reset_index(drop=True)
    df['index'] = np.arange(0, len(df))+1
    df['json_data'] = '{"' + df['index'].astype(str) + '":'+ df['data'] + "}"
    parsed_df = pd.concat(df['json_data'].apply(parse_json).tolist(), ignore_index=True)
    print('Hola')
    parsed_df = parsed_df.reset_index(drop=True)
    parsed_df= df[['timestamp', 'index']].join([parsed_df])
    results  = pd.DataFrame({'Row': parsed_df.columns})
    for index, row in results.iterrows():
        # Accede al valor en la columna 'NombresDeColumnas'
        valor_columna = row['Row']
        
        # Usa print para mostrar el valor
        print(f'Valor de la columna {index + 1}: {valor_columna}')
        
    #parsed_df = parsed_df.sort_values(by=['id'])
    # Define el nombre del archivo en GCS y el path local para guardar el archivo
    DATASET_NAME = 'raw_st'
    TABLE_NAME = 'parseo_df'
    table_id = f"{project}.{DATASET_NAME}.{TABLE_NAME}"
    print(parsed_df.head(2))
    # Carga el archivo CSV desde GCS a BigQuery
    load_job = client.load_table_from_dataframe(parsed_df, table_id)
    load_job.result()

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
         schedule_interval = "0 3 * * *") as dag: # schedule_interval = None # Caso sin trigger automático | schedule_interval = "0 12 * * *" | "0,2 12 * * *"

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
