-- Respostas às Questões de Negócio
-- Top 5 Campanhas (Leads, MQL, SAL) com quebra por ICP


SELECT
    campaign_last_touch,
    icp_score,
    COUNT(DISTINCT lead_id) as total_leads,
    COUNT(DISTINCT CASE WHEN converted_to_mql THEN lead_id END) as total_mql,
    COUNT(DISTINCT CASE WHEN converted_to_sal THEN lead_id END) as total_sal
FROM `case-2-boti.marketing_funnel.clean_funnel`
GROUP BY 1, 2
ORDER BY total_leads DESC -- Ou total_mql / total_sal dependendo do foco
LIMIT 5;


-- Top 5 Campanhas que geram mais leads tipo HR


SELECT
    campaign_last_touch,
    COUNT(DISTINCT lead_id) as total_leads_hr
FROM `case-2-boti.marketing_funnel.clean_funnel`
WHERE lead_type = 'HR' -- Hand Raise
GROUP BY 1
ORDER BY total_leads_hr DESC
LIMIT 5;

# Exportar resultado final para CSV
query = "SELECT * FROM `case-2-boti.marketing_funnel.tb_delivery_metrics`"
df_final = client.query(query).to_dataframe()
df_final.to_csv('entregavel_metricas_finais.csv', index=False, sep=';')
print("Arquivo CSV gerado com sucesso para envio.")
