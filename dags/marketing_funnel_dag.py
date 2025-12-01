from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
from google.cloud import bigquery
import os

# --- Configurações Padrão ---
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- Função Python de Ingestão
def run_ingestion_script():

    """Função para rodar o script de ingestão"""
    # Importa o script de ingestão
    # Nota: No Airflow real, as credenciais são gerenciadas via Connections, 
    # mas aqui mantenho o código simples para o desafio.

    client = bigquery.Client()
    
    # Lista de arquivos para ingerir (Exemplo)
    files = [
        {'path': '/path/to/data/bi_challenge_rd_bi_funnel_email.csv', 'table': 'raw_funnel'},
        {'path': '/path/to/data/metas_email.csv', 'table': 'raw_metas'}
    ]
    
    for file in files:
        df = pd.read_csv(file['path'])
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True
        )
        table_id = f"case-2-boti.marketing_funnel.{file['table']}"
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        print(f"Tabela {table_id} carregada.")

# --- Definição da DAG ---
with DAG(
    'marketing_funnel_pipeline',
    default_args=default_args,
    description='Pipeline de Marketing: Ingestão -> Clean -> Contract -> Delivery',
    schedule_interval='@daily', # Roda todo dia
    start_date=days_ago(1),
    tags=['marketing', 'case'],
    catchup=False
) as dag:

    # 1. Tarefa de Ingestão (Python)
    t1_ingestion = PythonOperator(
        task_id='ingest_raw_data',
        python_callable=run_ingestion_script
    )

    # 2. Tarefa de Limpeza (SQL - Silver)
    # Lê o arquivo SQL que criamos anteriormente
    with open('/dags/sql/etl_clean_funnel.sql', 'r') as f:
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

    # 3. Data Contract (Validação - Opcional do Desafio)
    # Se esta query falhar (RAISE ERROR), o pipeline para aqui.

    with open('/dags/sql/data_contract_check.sql', 'r') as f:
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
    
    with open('/dags/sql/etl_delivery_dashboard.sql', 'r') as f:
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