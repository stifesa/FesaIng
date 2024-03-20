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
import ast
import os
import io
import gspread
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
    
    def parse_json(x):
        json_data = json.loads(x)
        def get_nested_value(d, key, subkey):
            # Verificar si la clave principal existe y si tiene un subcampo
            if key in d and isinstance(d[key], dict):
                return d[key].get(subkey, '')
            else:
                return ''
        def get_deep_nested_value(d, keys):
        # Inicializar el valor temporal con el diccionario completo
            temp = d
            # Iterar a través de las claves para acceder al valor deseado
            for key in keys:
                # Verificar si la clave existe y si el valor asociado es un diccionario
                if key in temp and isinstance(temp[key], dict):
                    temp = temp[key]  # Acceder al siguiente nivel
                else:
                    return ''  # Retornar vacío si alguna clave intermedia no existe
            return temp  # Retornar el valor final si todas las claves existen
    
        # Extraer los campos deseados del JSON
        data = {
            'workOrder': json_data.get('workOrder', ''),
            'responsibleWorkshop': get_deep_nested_value(json_data, ['analysis', 'responsibleWorkshop', 'workshopName']),
            'id': json_data.get('id', ''),
            'eventType': json_data.get('eventType', ''),
            'state': json_data.get('state', ''),
            'workShop': json_data.get('workShop', ''),
            'miningOperation': json_data.get('miningOperation', ''),
            'packageNumber': json_data.get('packageNumber', ''),
            'question1': json_data.get('question1', ''),
            'enventDetail': json_data.get('enventDetail', ''),
            'component': json_data.get('component', ''),
            'partNumber': json_data.get('partNumber', ''),
            'generalImages': json_data.get('generalImages', ''),
            'correctiveActions': json_data.get('correctiveActions', ''),
            'observation': json_data.get('analysis', {}).get('observation', ''),
            'process': json_data.get('analysis', {}).get('process', ''),
            'bahia': json_data.get('analysis', {}).get('bahia', ''),
            'basicCause': json_data.get('analysis', {}).get('basicCause', ''),
            'responsable': json_data.get('analysis', {}).get('responsable', ''),
            'causeFailure': json_data.get('analysis', {}).get('causeFailure', ''),
            'processAt': json_data.get('processAt', {}).get('_seconds', ''),
            'finalizedAt': json_data.get('finalizedAt', {}).get('_seconds', ''),
            'tracingAt': json_data.get('tracingAt', {}).get('_seconds', ''),
            'createdBy_email': json_data.get('createdBy', {}).get('email', ''),
            'specialist': json_data.get('specialist', ''),
            'reportingWorkshop': get_nested_value(json_data, 'reportingWorkshop', 'workshopName'),
            'specialist': get_nested_value(json_data, 'specialist', 'name'),
            'createdAt': json_data.get('createdAt', {}).get('_seconds', '')
        }
        return pd.Series(data)
    
    def get_nested_value(d, key, subkey):
        # Verificar si d es un diccionario, si la clave principal existe y si tiene un subcampo
        if isinstance(d, dict) and key in d and isinstance(d[key], dict):
            return d[key].get(subkey, '')
        else:
            return ''
    
    def get_deep_nested_value(d, keys):
        # Verificar si d es un diccionario antes de iniciar
        if not isinstance(d, dict):
            return ''
        temp = d
        for key in keys:
            if key in temp and isinstance(temp[key], dict):
                temp = temp[key]
            else:
                return ''
        return temp

    def extract_json_values(row):
        try:
            data_list = ast.literal_eval(row)  # Convertir la lista de texto a una lista de Python
            values = []
            for item in data_list:
                if 'corrective' in item:
                    corrective = item.get('corrective', '')
                    closedAt = get_nested_value(item, 'closedAt', '_seconds')
                    createdAt = get_nested_value(item, 'createdAt', '_seconds')
                    values.append({'corrective': corrective, 'closedAt_seconds': closedAt, 'createdAt_seconds': createdAt})
            return values
        except Exception as e:
            print("Error:", e)
            return []

    df = client.query(sql).to_dataframe()
    # Realiza transformaciones en el DataFrame
    df = df.reset_index(drop=True)
    df['index'] = np.arange(0, len(df))+1
    #df['json_data'] = '{"' + df['index'].astype(str) + '":'+ df['data'] + "}"
    df['json_data'] = df['data']
    df_parsed = df['json_data'].apply(parse_json)
    
    #df_final = df_final.reset_index(drop=True)
    df_final = pd.concat([df, df_parsed], axis=1)
    df_final.sort_values(['timestamp', 'id'], ascending=[False, True], inplace=True)
    df_final = pd.DataFrame(df_final)
    df_final['finalizedAt'] = df_final['finalizedAt'].fillna(0)
    df_final['tracingAt'] = df_final['tracingAt'].fillna(0)
    df_final['processAt'] = df_final['processAt'].fillna(0)
    df_final['creacion'] = pd.to_datetime(df_final['createdAt'], unit='s')
    df_final['creacion'] = df_final['creacion'].dt.strftime("%d/%m/%Y %H:%M:%S")
    df_final['finalizacion'] = pd.to_datetime(df_final['finalizedAt'], unit='s')
    df_final['finalizacion'] = df_final['finalizacion'].dt.strftime("%d/%m/%Y %H:%M:%S")
    df_final['proceso_inicio'] = pd.to_datetime(df_final['processAt'], unit='s')
    df_final['proceso_inicio'] = df_final['proceso_inicio'].dt.strftime("%d/%m/%Y %H:%M:%S")
    df_final['seguimiento'] = pd.to_datetime(df_final['tracingAt'], unit='s')
    df_final['seguimiento'] = df_final['seguimiento'].dt.strftime("%d/%m/%Y %H:%M:%S")
    df_final['Rank'] = 1
    df_final['Rank'] = df_final.groupby(['id'])['Rank'].cumsum()
    n_by_iddata = df_final.loc[(df_final.Rank == 1)]
    quality=n_by_iddata[['id','eventType','state','timestamp','creacion','finalizacion','seguimiento','index','state','workOrder','workShop','partNumber','generalImages','miningOperation','specialist','enventDetail','observation','process','bahia','basicCause','responsable','causeFailure','reportingWorkshop','createdBy_email','correctiveActions','component','proceso_inicio','question1','packageNumber','responsibleWorkshop']]
    quality.columns = ['id','TipoEvento','estado_final','Ultima_mod', 'Fecha_creacion','Fecha_fin','fecha_seguimiento', 'indice','estado','workorder','Taller','NumParte','Imagen','OperacionMin','Especialista','DetalleEvento','Observacion','Proceso','Bahia','CausaBasica','Responsable','CausaFalla','TallerReporta','email_registro','AccionCorrectiva','component','proceso_inicio','question1','plaqueteo','responsibleWorkshop']
    #quality = quality.drop_duplicates()
    # Define el nombre del archivo en GCS y el path local para guardar el archivo
    DATASET_NAME = 'raw_st'
    TABLE_NAME = 'dsp_calidad'
    table_id = f"{project}.{DATASET_NAME}.{TABLE_NAME}"
    quality.reset_index(inplace=True, drop=True)
    # Remove square brackets from the 'col1' column
    #quality['AccionCorrectiva'] = quality['AccionCorrectiva'].str.replace('[', '').str.replace(']', '')
    quality['AccionCorrectiva'] = quality['AccionCorrectiva'].str.replace("'", '"')
    quality['AccionCorrectiva'] = quality['AccionCorrectiva'].astype(str)
    print(quality['AccionCorrectiva'])
    correctivos = quality[quality['AccionCorrectiva'].apply(lambda x: isinstance(x, str) and x != '[]' and x != 'nan')]
    print(correctivos.head())
    print(quality.head(2))
    correctivos['json_values'] = correctivos['AccionCorrectiva'].apply(extract_json_values)
    new_rows = []
    for i, row in correctivos.iterrows():
        for value in row['json_values']:
            new_row = row.copy()
            new_row.update(value)
            new_rows.append(new_row)

    # Crear un nuevo DataFrame con las filas divididas
    new_df = pd.DataFrame(new_rows)
    print(new_df.head())
    #correctivos[['corrective', 'created_at', 'closed_at']] = pd.DataFrame(correctivos['parsed_acciones'].tolist(), index=correctivos.index)
    #print(correctivos[['corrective']].head())
    quality = quality.astype(str)

    #Lectura de datos de BD AFA
    gc2 = gspread.authorize(credentials)
    bdafa = gc2.open_by_key('1XPiLhawZzAIHiplJQp5olCTcPkPvXGwXo2My4BYVduc')
    sheet_list = bdafa.worksheets()
    sheet_instance1 = bdafa.get_worksheet_by_id(902604264)
    #sheet_instance1=bdafa.get_worksheet('BD_AFAS')
    afa = sheet_instance1.get_all_records()
    afa = pd.DataFrame.from_dict(afa)
    bdafa= afa[['N° CASO PAT-AFA','ESTADO','INICIO DESARMADO','EMISION','ESTADO','OT MAIN','TALLER','PN FALLA','ANALISTA','Modo de Falla','Causa','CARGO TÉCNICO AFA','TALLER','COMPONENTE','AGRUPADOR']]
    bdafa.columns = ['id','estado_final','Fecha_creacion','Fecha_fin','estado','workorder','Taller','NumParte','Especialista','DetalleEvento','CausaBasica','Responsable','TallerReporta','component','responsibleWorkshop']
    bdafa['TipoEvento'] = 'SNC AFA'
    oportunidades = pd.concat([quality, bdafa], ignore_index=True)
    oportunidades = oportunidades.astype(str)
    #
    sqltrunc = """
    TRUNCATE TABLE `ferreyros-mvp.raw_st.dsp_calidad`
    """
    client.query(sqltrunc)
    # Carga el archivo CSV desde GCS a BigQuery
    load_job = client.load_table_from_dataframe(oportunidades, table_id)
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
         schedule_interval = "0 */4 * * *") as dag: # schedule_interval = None # Caso sin trigger automático | schedule_interval = "0 12 * * *" | "0,2 12 * * *"

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
