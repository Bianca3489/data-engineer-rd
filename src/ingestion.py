import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.colab import drive
import os

# Isso é necessário para acessar o JSON de credenciais e os arquivos CSV
print("Montando Google Drive...")
drive.mount('/content/drive')


PROJECT_ID = 'case-2-boti'
DATASET_ID = 'marketing_funnel'

# Caminhos dos arquivos
# Assumi que estão soltos na raiz do seu "Meu Drive"
BASE_PATH = '/content/drive/MyDrive' 

CREDENTIALS_PATH = os.path.join(BASE_PATH, 'credentials.json')
FILE_PATH_FUNNEL = os.path.join(BASE_PATH, 'bi_challenge_rd_bi_funnel_email.csv')
FILE_PATH_METAS = os.path.join(BASE_PATH, 'metas_email.csv')

# Verifica se o arquivo de credenciais existe antes de tentar
if not os.path.exists(CREDENTIALS_PATH):
    raise FileNotFoundError(f"Arquivo de credenciais não encontrado em: {CREDENTIALS_PATH}")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
client = bigquery.Client(project=PROJECT_ID)

# Funções Auxiliares 

def create_dataset_if_not_exists(dataset_id):
    """Cria o dataset no BigQuery se ele não existir"""
    dataset_ref = f"{PROJECT_ID}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} já existe.")
    except Exception:
        print(f"Dataset {dataset_id} não encontrado. Criando...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US" # Ou "southamerica-east1" se preferir
        client.create_dataset(dataset)
        print(f"Dataset {dataset_id} criado com sucesso.")

def load_csv_to_bigquery(file_path, table_name):
    """Carrega CSV para o BigQuery (Camada RAW)"""
    
    # Verifica se o arquivo CSV existe
    if not os.path.exists(file_path):
        print(f"ERRO: O arquivo não foi encontrado no caminho: {file_path}")
        print("Verifique se o nome está correto ou se ele está na pasta certa do Google Drive.")
 import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.colab import drive
import os

# Acessa o JSON de credenciais e os arquivos CSV
print("Montando Google Drive...")
drive.mount('/content/drive')


PROJECT_ID = 'case-2-boti'
DATASET_ID = 'marketing_funnel'

# Caminhos dos arquivos
#  Assumi que estão soltos na raiz do seu "Meu Drive"
BASE_PATH = '/content/drive/MyDrive' 

CREDENTIALS_PATH = os.path.join(BASE_PATH, 'credentials.json')
FILE_PATH_FUNNEL = os.path.join(BASE_PATH, 'bi_challenge_rd_bi_funnel_email.csv')
FILE_PATH_METAS = os.path.join(BASE_PATH, 'metas_email.csv')

# Verifica se o arquivo de credenciais existe antes de tentar
if not os.path.exists(CREDENTIALS_PATH):
    raise FileNotFoundError(f"Arquivo de credenciais não encontrado em: {CREDENTIALS_PATH}")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
client = bigquery.Client(project=PROJECT_ID)

# Funções Auxiliares 

def create_dataset_if_not_exists(dataset_id):
    """Cria o dataset no BigQuery se ele não existir"""
    dataset_ref = f"{PROJECT_ID}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} já existe.")
    except Exception:
        print(f"Dataset {dataset_id} não encontrado. Criando...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US" 
        client.create_dataset(dataset)
        print(f"Dataset {dataset_id} criado com sucesso.")

def load_csv_to_bigquery(file_path, table_name):
    """Carrega CSV para o BigQuery (Camada RAW)"""
    
    # Verifica se o arquivo CSV existe
    if not os.path.exists(file_path):
        print(f"ERRO: O arquivo não foi encontrado no caminho: {file_path}")
        print("Verifique se o nome está correto ou se ele está na pasta certa do Google Drive.")
        return

    print(f"Lendo arquivo: {file_path}...")
    # Leitura do CSV
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Erro ao ler o CSV com Pandas: {e}")
        return

    print(f"Carregando para o BigQuery: {DATASET_ID}.{table_name}...")
    
    # Configuração do Job
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE", # Sobrescreve a tabela se existir
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True # Detecta schema automaticamente
    )
    
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    
    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Aguarda conclusão
        print(f"✅ Sucesso! Tabela {table_name} carregada. {job.output_rows} linhas inseridas.\n")
    except Exception as e:
        print(f"❌ Erro ao enviar para o BigQuery: {e}")

# Execução Principal -
if __name__ == "__main__":
    
    # 1. Garante que o Dataset existe
    create_dataset_if_not_exists(DATASET_ID)
    
    # 2. Carrega Funnel
    load_csv_to_bigquery(FILE_PATH_FUNNEL, 'raw_funnel')
    
    # 3. Carrega Metas
    load_csv_to_bigquery(FILE_PATH_METAS, 'raw_metas')       return

    print(f"Lendo arquivo: {file_path}...")
    # Leitura do CSV
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Erro ao ler o CSV com Pandas: {e}")
        return

    print(f"Carregando para o BigQuery: {DATASET_ID}.{table_name}...")
    
    # Configuração do Job
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE", # Sobrescreve a tabela se existir
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True # Detecta schema automaticamente
    )
    
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    
    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Aguarda conclusão
        print(f"✅ Sucesso! Tabela {table_name} carregada. {job.output_rows} linhas inseridas.\n")
    except Exception as e:
        print(f"❌ Erro ao enviar para o BigQuery: {e}")

# --- PASSO 5: Execução Principal ---
if __name__ == "__main__":
    
    # 1. Garante que o Dataset existe
    create_dataset_if_not_exists(DATASET_ID)
    
    # 2. Carrega Funnel
    load_csv_to_bigquery(FILE_PATH_FUNNEL, 'raw_funnel')
    
    # 3. Carrega Metas
    load_csv_to_bigquery(FILE_PATH_METAS, 'raw_metas')