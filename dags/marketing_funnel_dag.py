from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
from google.cloud import bigquery
import os

# --- 1. CONFIGURAÇÃO INTELIGENTE DE CAMINHOS ---
# Descobre onde este arquivo .py está (ex: .../desafio-data-engineer/dags)
DAGS_FOLDER = os.path.dirname(os.path.abspath(__file__))

# Descobre a raiz do projeto (sobe um nível: .../desafio-data-engineer)
PROJECT_ROOT = os.path.dirname(DAGS_FOLDER)

# --- Configurações Padrão ---
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- Função Python de Ingestão ---
def run_ingestion_script():
    """Lê os CSVs da raiz do projeto e sobe para o BigQuery"""
    
    client = bigquery.Client()
    
    # Monta o caminho exato onde os CSVs estão (Na raiz do projeto)
    csv_funnel = os.path.join(PROJECT_ROOT, 'bi_challenge_rd_bi_funnel_email.csv')
    csv_metas = os.path.join(PROJECT_ROOT, 'metas_email.csv')

    # Lista de arquivos para ingerir
    files = [
        {'path': csv_funnel, 'table': 'raw_funnel'},
        {'path': csv_metas, 'table': 'raw_metas'}
    ]
    
    for file in files:
        print(f"Procurando arquivo em: {file['path']}")
        
        # Validação de Segurança: O arquivo existe?
        if not os.path.exists(file['path']):
            raise FileNotFoundError(f"ERRO CRÍTICO: Não achei o arquivo {file['path']}. Verifique se ele está na pasta {PROJECT_ROOT}")

        print(f"Lendo CSV...")
        df = pd.read_csv(file['path'])
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True
        )
        table_id = f"case-2-boti.marketing_funnel.{file['table']}"
        
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f"✅ Sucesso: Tabela {table_id} carregada.")

# --- Definição da DAG ---
with DAG(
    'marketing_funnel_pipeline',
    default_args=default_args,
    description='Pipeline de Marketing: Ingestão -> Clean -> Contract -> Delivery',
    schedule_interval='@daily',
    start_date=days_ago(1),
    tags=['marketing', 'case'],
    catchup=False
) as dag:

    # 1. Tarefa de Ingestão
    t1_ingestion = PythonOperator(
        task_id='ingest_raw_data',
        python_callable=run_ingestion_script
    )

    # 2. Tarefa de Limpeza (SQL - Silver)
    # Procura o SQL dentro de dags/sql/
    path_clean = os.path.join(DAGS_FOLDER, 'sql/etl_clean_funnel.sql')
    
    with open(path_clean, 'r') as f:
        sql_clean = f.read()

    t2_transform_silver = BigQueryInsertJobOperator(
        task_id='transform_silver_layer',
        configuration={
            "query": {
                "query": sql_clean,
                "useLegacySql": False,
            }
        },
        location='US',
    )

    # 3. Data Contract (Validação)
    path_contract = os.path.join(DAGS_FOLDER, 'sql/data_contract_check.sql')

    with open(path_contract, 'r') as f:
        sql_contract = f.read()

    t3_data_contract = BigQueryInsertJobOperator(
        task_id='check_data_contract',
        configuration={
            "query": {
                "query": sql_contract,
                "useLegacySql": False,
            }
        },
        location='US',
    )

    # 4. Tarefa de Entrega (SQL - Gold)
    path_delivery = os.path.join(DAGS_FOLDER, 'sql/etl_delivery_dashboard.sql')
    
    with open(path_delivery, 'r') as f:
        sql_delivery = f.read()

    t4_transform_gold = BigQueryInsertJobOperator(
        task_id='transform_gold_layer',
        configuration={
            "query": {
                "query": sql_delivery,
                "useLegacySql": False,
            }
        },
        location='US',
    )

    # --- Ordem de Execução ---
    t1_ingestion >> t2_transform_silver >> t3_data_contract >> t4_transform_gold