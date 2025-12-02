CREATE OR REPLACE TABLE `case-2-boti.marketing_funnel.tb_delivery_metrics` AS

WITH daily_metrics AS (
    -- Agrupa dados reais por dia e dimensões
    SELECT
        DATE(lead_created_at) as referencia_data,
        DATE_TRUNC(DATE(lead_created_at), MONTH) as referencia_mes, 
        account_last_touch as account,
        icp_score,
        lead_type,
        country,
        
        -- Contagens (Actuals)
        COUNT(DISTINCT lead_id) as qtd_leads,
        COUNT(DISTINCT CASE WHEN converted_to_mql THEN lead_id END) as qtd_mql,
        COUNT(DISTINCT CASE WHEN converted_to_sal THEN lead_id END) as qtd_sal,
        
        -- E-mails únicos
        COUNT(DISTINCT lead_id) as qtd_emails_unicos,
        
        -- Último horário de conversão do dia
        MAX(TIME(lead_created_at)) as ultimo_horario_conversao
    FROM `case-2-boti.marketing_funnel.clean_funnel`
    GROUP BY 1, 2, 3, 4, 5, 6
),

metas_pivoted AS (
    -- ETAPA DE CORREÇÃO: Transformar linhas em colunas (Pivot)
    SELECT
        PARSE_DATE('%d/%m/%Y', data) as mes_meta,
        conta,
        perfil,
        
        -- Pivotagem
        SUM(CASE WHEN etapa = 'Lead' THEN meta ELSE 0 END) as goal_leads,
        SUM(CASE WHEN etapa = 'MQL'  THEN meta ELSE 0 END) as goal_mql,
        SUM(CASE WHEN etapa = 'SAL'  THEN meta ELSE 0 END) as goal_sal
    FROM `case-2-boti.marketing_funnel.raw_metas`
    GROUP BY 1, 2, 3
),

metas_finais AS (
    -- Calcula a meta diária (Meta Mensal / 30)
    SELECT
        mes_meta,
        conta,
        perfil,
        goal_leads,
        goal_mql,
        goal_sal,
        goal_leads / 30 as meta_leads_diaria,
        goal_mql / 30 as meta_mql_diaria,
        goal_sal / 30 as meta_sal_diaria
    FROM metas_pivoted
)

SELECT
    d.*,
    
    -- Metas Diárias
    m.meta_leads_diaria,
    m.meta_mql_diaria,
    m.meta_sal_diaria,
    
    -- Metas Mensais
    m.goal_leads as meta_leads_mensal,
    m.goal_mql as meta_mql_mensal,
    m.goal_sal as meta_sal_mensal,

    -- Cálculo Month-to-Date (MTD)
    SUM(d.qtd_leads) OVER (PARTITION BY d.referencia_mes, d.account, d.icp_score, d.lead_type, d.country ORDER BY d.referencia_data) as mtd_leads,
    SUM(d.qtd_mql) OVER (PARTITION BY d.referencia_mes, d.account, d.icp_score, d.lead_type, d.country ORDER BY d.referencia_data) as mtd_mql,
    SUM(d.qtd_sal) OVER (PARTITION BY d.referencia_mes, d.account, d.icp_score, d.lead_type, d.country ORDER BY d.referencia_data) as mtd_sal,
    
    -- Taxas de Conversão
    SAFE_DIVIDE(d.qtd_mql, d.qtd_leads) as taxa_conv_lead_mql,
    SAFE_DIVIDE(d.qtd_sal, d.qtd_mql) as taxa_conv_mql_sal

FROM daily_metrics d
LEFT JOIN metas_finais m 
    ON d.referencia_mes = m.mes_meta
    AND d.account = m.conta
    AND d.icp_score = m.perfil;