CREATE OR REPLACE TABLE `case-2-boti.marketing_funnel.clean_funnel` AS
WITH deduplicated AS (
    SELECT 
        *,
        -- Regra: Se houver IDs repetidos, manter apenas um (usamos row_number)
        ROW_NUMBER() OVER(PARTITION BY id ORDER BY lead_created_date DESC) as rn
    FROM `case-2-boti.marketing_funnel.raw_funnel`
    WHERE 
        -- Regra: Remoção de registros inválidos com Id vazio
        id IS NOT NULL AND id <> ''
)
SELECT
     id as lead_id,
    -- Regra: Converter timezone UTC para UTC-3 (America/Sao_Paulo) e tipar dados
    DATETIME(TIMESTAMP(lead_created_date), 'America/Sao_Paulo') as lead_created_at,
    DATETIME(TIMESTAMP(mql_created_date), 'America/Sao_Paulo') as mql_created_at,
    DATETIME(TIMESTAMP(sal_created_date), 'America/Sao_Paulo') as sal_created_at,
    
    -- Converter mês de criação
    PARSE_DATE('%Y-%m-%d', lead_created_month) as lead_created_month,

    -- Strings e Booleanos
    CAST(campaign_last_touch AS STRING) as campaign_last_touch,
    CAST(identifier_last_touch AS STRING) as identifier_last_touch,
    CAST(account_last_touch AS STRING) as account_last_touch,
    CAST(icp_score AS STRING) as icp_score,
    CAST(lead_type AS STRING) as lead_type,
    CAST(country AS STRING) as country,
    
    -- Flags booleanas
    CAST(COALESCE(converted_to_mql, 0) AS BOOL) as converted_to_mql,
    CAST(COALESCE(converted_to_sal, 0) AS BOOL) as converted_to_sal
FROM deduplicated
WHERE rn = 1 
-- NOVO FILTRO: Garante integridade temporal
  AND (mql_created_date >= lead_created_date OR mql_created_date IS NULL)
  AND (sal_created_date >= lead_created_date OR sal_created_date IS NULL);