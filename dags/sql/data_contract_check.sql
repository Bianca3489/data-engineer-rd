/*
  DATA CONTRACT VALIDATION
  Regras:
  1. ID do Lead não pode ser nulo.
  2. A data de MQL não pode ser anterior à data de criação do Lead (consistência temporal).
  3. O campo icp_score deve conter apenas A, B, C ou D.
*/

BEGIN
  DECLARE erros_ids INT64;
  DECLARE erros_temporal INT64;
  DECLARE erros_domain INT64;

  -- Regra 1: IDs Nulos
  SET erros_ids = (SELECT COUNT(*) FROM `case-2-boti.marketing_funnel.clean_funnel` WHERE lead_id IS NULL);

  -- Regra 2: Viagem no tempo (MQL antes do Lead)
  SET erros_temporal = (
      SELECT COUNT(*) 
      FROM `case-2-boti.marketing_funnel.clean_funnel` 
      WHERE mql_created_at < lead_created_at
  );

  -- Regra 3: Valores fora do domínio permitido
  SET erros_domain = (
      SELECT COUNT(*)
      FROM `case-2-boti.marketing_funnel.clean_funnel`
      WHERE icp_score NOT IN ('A', 'B', 'C', 'D') AND icp_score IS NOT NULL
  );

  -- Validação Final
  IF erros_ids > 0 OR erros_temporal > 0 OR erros_domain > 0 THEN
      RAISE USING MESSAGE = FORMAT('DATA CONTRACT FAILED: IDs Nulos: %d, Erros Temporais: %d, Domínio Inválido: %d', erros_ids, erros_temporal, erros_domain);
  ELSE
      SELECT 'Data Contract: Passed' as status;
  END IF;
END;