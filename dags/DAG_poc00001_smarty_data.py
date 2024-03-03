# ALNETAHU - 2024

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery
from googleapiclient.discovery import build
from google.oauth2 import service_account
import json
import io
import numpy as np


#######################################################################################
# PARAMETROS
#######################################################################################
nameDAG           = 'DAG_poc00001_smarty_data'
project           = 'ferreyros-mvp'
owner             = 'ALNETAHU'
email             = ['astroboticapps@gmail.com']
GBQ_CONNECTION_ID = 'bigquery_default'
#######################################################################################

def process_and_load_data(**kwargs):
    # Configura el path donde tu archivo CSV será almacenado
    folder_path = '/content/gdrive/My Drive/CIT - CENTRAL DE INFORMACIÓN TÉCNICA/LANZAMIENTO SMARTY/SMARTY DATA
    csv_file = [f for f in os.listdir(folder_path) if 'KIT_CONTENIDO' in f and f.endswith('.csv')]

    if not csv_file:
        raise FileNotFoundError("No se encontró el archivo CSV con el patrón 'KIT_CONTENIDO' en su nombre.")
    
    # Asume que solo hay un archivo que coincide
    csv_path = os.path.join(folder_path, csv_file[0])

    # Lee el archivo CSV en un DataFrame de pandas
    df = pd.read_csv(csv_path)

    # Aquí puedes realizar las transformaciones necesarias en el DataFrame
    # Por ejemplo: df = df.transform(...)

    # Autenticación con Google Cloud BigQuery
    credentials = service_account.Credentials.from_service_account_file(
        '/content/gdrive/My Drive/Proyectos/Credencial/ferreyros-mvp-3cf04ce5fdcc.json'
    )

    # Cliente de BigQuery
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # Nombre de la tabla en BigQuery donde se cargarán los datos
    table_id = 'ferreyros-mvp.raw_st.pre_smarty'

    # Carga los datos en BigQuery
    job = client.load_table_from_dataframe(df, table_id)
    job.result()  # Espera a que la carga se complete



default_args = {
    'owner': owner,                   # The owner of the task.
    'depends_on_past': False,         # Task instance should not rely on the previous task's schedule to succeed.
    'start_date': datetime.datetime(2022, 11, 5),
    'email': email,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,  # Retry once before failing the task.
    'retry_delay': datetime.timedelta(minutes=1),  # Time between retries
    'project_id': ferreyros-mvp,  # Cloud Composer project ID.
}

with DAG(nameDAG,
         default_args = default_args,
         catchup = False,  # Ver caso catchup = True
         max_active_runs = 3,
         schedule_interval = "30 1/6 * * *") as dag: # schedule_interval = None # Caso sin trigger automático | schedule_interval = "0 12 * * *" | "0,2 12 * * *"

    # FUENTE: CRONTRAB: https://crontab.guru/
    #############################################################
    
    t_begin = DummyOperator(task_id="begin")
    
    task_python = PythonOperator(task_id='task_python',
                                 provide_context=True,
                                 python_callable=process_and_load_data,
                                 op_kwargs={
                                    'numeric_input': np.pi,
                                    'var1': "Variable1"
                                    }
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
