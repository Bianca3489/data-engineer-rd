# Desafio Técnico de Engenharia de Dados - Email Marketing Funnel

## 📋 Resumo do Projeto
Este projeto consiste na construção de um pipeline de dados (ELT) para processar métricas de Email Marketing. O objetivo é ingerir dados brutos, tratá-los, validar sua qualidade e entregar tabelas analíticas para acompanhamento de metas (Leads, MQLs, SALs) e conversões.

A solução foi implementada utilizando **Google BigQuery** como Data Warehouse, **Python** para ingestão e **SQL** para transformações seguindo a arquitetura Medallion (Bronze, Silver, Gold).

## 🛠️ Tecnologias Utilizadas
* **Linguagem:** Python 3.9+ & SQL (Standard dialect)
* **Data Warehouse:** Google BigQuery
* **Orquestração:** Apache Airflow (DAGs incluídas)
* **Versionamento:** Git

## 🏗️ Arquitetura de Dados (Medallion)

O pipeline foi dividido em três camadas lógicas:

1.  **Bronze (Raw):** Ingestão dos arquivos CSV (`raw_funnel`, `raw_metas`) sem tratamento.
2.  **Silver (Clean):**
    * Deduplicação de registros (mantendo o mais recente).
    * Conversão de tipos (Casting).
    * Padronização de Timezone (UTC -> America/Sao_Paulo).
    * **Data Quality:** Remoção de inconsistências temporais (ex: MQL criado antes do Lead).
3.  **Gold (Delivery):**
    * Cálculo de métricas acumuladas (Month-to-Date).
    * Pivotagem da tabela de metas para comparação diária.
    * Join entre dados realizados (Actual) e orçados (Budget).

## 🚀 Como Executar o Projeto

### Pré-requisitos
* Conta no Google Cloud Platform (GCP) com BigQuery habilitado.
* Service Account com permissão de `BigQuery Admin`.
* Python 3 instalado.

### Passo a Passo

1.  **Instalação das dependências:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Configuração de Credenciais:**
    * Coloque o arquivo `credentials.json` (Service Account Key) na raiz do projeto.
    * *Nota: Por questões de segurança, este arquivo não está versionado no Git.*

3.  **Execução Manual (Ingestão):**
    ```bash
    python src/ingestion.py
    ```

4.  **Execução das Transformações (SQL):**
    * Rodar `sql/etl_clean_funnel.sql` no console do BigQuery.
    * Rodar `sql/data_contract_check.sql` para validar integridade.
    * Rodar `sql/etl_delivery_dashboard.sql` para gerar a tabela final.

### 🔄 Orquestração (Opcional)
Foi desenhada uma DAG do **Airflow** (`dags/marketing_funnel_dag.py`) que automatiza o fluxo:
`Ingestão -> Limpeza (Silver) -> Data Contract Check -> Entrega (Gold)`

## ✅ Qualidade de Dados (Data Contracts)
Foi implementada uma etapa de validação (`sql/data_contract_check.sql`) que atua como *Circuit Breaker*. O pipeline é interrompido caso:
* Existam IDs nulos.
* **Inconsistência Temporal:** Data de conversão MQL anterior à criação do Lead.
* Valores de ICP Score fora do domínio (A-D).

## 📊 Entregáveis de Negócio
As queries para responder às perguntas estratégicas (Top Campanhas, Leads HR) encontram-se no arquivo:
📂 `sql/business_questions.sql`

O CSV final com os dados processados para o Dashboard está disponível na raiz como:
📄 `entregavel_metricas_finais.csv`

---
Desenvolvido por **Bianca Rodrigues Soares Costa**