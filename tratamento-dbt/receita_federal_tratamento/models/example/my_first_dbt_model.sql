
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

WITH estabelecimentos AS (
SELECT
    e.*,
    SPLIT(SPLIT(UPPER(e.EMAIL), '@')[SAFE_OFFSET(1)], '.')[SAFE_OFFSET(0)] AS PROVEDOR,
    LOWER(e.EMAIL) AS EMAIL_LOWER,
    m.MUNICIPIO AS MUNICIPIO_NOME,
    p.NM_PAIS,
    c.CNAE,
    mo.NM_MOTIVO,
    CASE
      WHEN SIT_CADASTRAL = 1 THEN 'NULA'
      WHEN SIT_CADASTRAL = 2 THEN 'ATIVA'
      WHEN SIT_CADASTRAL = 3 THEN 'SUSPENSA'
      WHEN SIT_CADASTRAL = 4 THEN 'INAPTA'
      WHEN SIT_CADASTRAL = 8 THEN 'BAIXADA'
      ELSE 'OUTRO'
    END AS NM_SIT_CADASTRAL,
    SAFE.PARSE_DATE('%Y%m%d', e.DT_SIT_CADASTRAL) AS DT_SIT_CADASTRAL_DATE,
    SAFE.PARSE_DATE('%Y%m%d', e.DT_INICIO_ATIVIDADE) AS DT_INICIO_ATIVIDADE_DATE
FROM
    `kinetic-valor-414812.receitafederal.estabelecimentos` e
LEFT JOIN
    `kinetic-valor-414812.receitafederal.municipios` m ON e.MUNICIPIO = m.ID_MUNICPIO
LEFT JOIN
    `kinetic-valor-414812.receitafederal.paises` p ON e.PAIS = p.COD_PAIS
LEFT JOIN
    `kinetic-valor-414812.receitafederal.cnaes` c ON e.CNAE_1 = c.COD_CNAE
LEFT JOIN
    `kinetic-valor-414812.receitafederal.motivos` mo ON e.MOTIVO_CADASTRAL = mo.COD_MOTIVO
)
SELECT
  *,
  CONCAT(TIPO_LOUGRADOURO, ' ', LOGRADOURO, ', ', NUMERO, ' - ', MUNICIPIO_NOME, ', ', UF) AS ENDERECO_COMPLETO,
  EXTRACT(YEAR FROM DT_SIT_CADASTRAL_DATE) AS ANO_SIT_CADASTRAL,
  EXTRACT(MONTH FROM DT_SIT_CADASTRAL_DATE) AS MES_SIT_CADASTRAL
FROM
  estabelecimentos es
LEFT JOIN `kinetic-valor-414812.receitafederal.dicionario_provedores` dp ON es.PROVEDOR = dp.PROVEDOR_VALIDO


/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
