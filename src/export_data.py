import os
import pandas as pd
from google.cloud import bigquery

# --- Configurações --

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CREDENTIALS_PATH = os.path.join(BASE_DIR, 'credentials.json')
OUTPUT_FILE = os.path.join(BASE_DIR, 'entregavel_metricas_finais.csv')

# Configuração do BigQuery
PROJECT_ID = 'case-2-boti'
DATASET_ID = 'marketing_funnel'
TABLE_ID = 'tb_delivery_metrics'

def export_data():
    print("--- Iniciando Exportação de Dados ---")
    
    # 1. Autenticação
    if not os.path.exists(CREDENTIALS_PATH):
        raise FileNotFoundError(f"Erro: Arquivo de credenciais não encontrado em {CREDENTIALS_PATH}")
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
    client = bigquery.Client()

    # 2. Query para pegar todos os dados da tabela final (Gold)
    query = f"""
        SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        ORDER BY referencia_data DESC
    """
    
    print(f"Lendo dados do BigQuery: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}...")
    
    try:
        # Baixa os dados para um DataFrame Pandas
        df = client.query(query).to_dataframe()
        
        # 3. Salva em CSV
        print(f"Salvando {len(df)} linhas em CSV...")
        
        # index=False (não salva o número da linha), sep=',' (padrão internacional)
        # sep=';' (padrão Excel Brasil, se preferir, mude aqui)
        df.to_csv(OUTPUT_FILE, index=False, sep=',')
        
        print(f"✅ Sucesso! Arquivo gerado na raiz: {OUTPUT_FILE}")
        
    except Exception as e:
        print(f"❌ Erro ao exportar dados: {e}")

if __name__ == "__main__":
    export_data()