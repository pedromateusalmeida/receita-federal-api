
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
    SPLIT(SPLIT(LOWER(e.EMAIL), '@')[SAFE_OFFSET(1)], '.')[SAFE_OFFSET(0)] AS PROVEDOR,
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
    FORMAT_DATE('%Y-%m-%d', SAFE.PARSE_DATE('%Y%m%d', DT_SIT_CADASTRAL)) AS DT_SIT_CADASTRAL_FORMATADA,
    FORMAT_DATE('%Y-%m-%d', SAFE.PARSE_DATE('%Y%m%d', DT_INICIO_ATIVIDADE)) AS DT_INICIO_ATIVIDADE_FORMATADA
  FROM
    `kinetic-valor-414812.receitafederal.estabelecimentos` e
  LEFT JOIN
    `kinetic-valor-414812.receitafederal.municipios` m
    ON e.MUNICIPIO = m.ID_MUNICPIO
  LEFT JOIN
    `kinetic-valor-414812.receitafederal.paises` p
    ON e.PAIS = p.COD_PAIS
  LEFT JOIN
    `kinetic-valor-414812.receitafederal.cnaes` c
    ON e.CNAE_1 = c.COD_CNAE
  LEFT JOIN
    `kinetic-valor-414812.receitafederal.motivos` mo
    ON e.MOTIVO_CADASTRAL = mo.COD_MOTIVO
)
SELECT
  *,
  CONCAT(TIPO_LOUGRADOURO, ' ', LOGRADOURO, ', ', NUMERO, ' - ', MUNICIPIO_NOME, ', ', UF) AS ENDERECO_COMPLETO
FROM
  estabelecimentos

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
