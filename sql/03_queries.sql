WITH base AS (
  SELECT
    d.cnpj,
    d.razao_social,
    d.ano,
    d.trimestre,
    d.vl_saldo_final,
    (d.ano * 10 + d.trimestre) AS periodo
  FROM despesas_consolidadas d
  WHERE d.cnpj IS NOT NULL
    AND d.cnpj <> ''
),
first_last AS (
  SELECT
    cnpj,
    razao_social,
    FIRST_VALUE(vl_saldo_final) OVER (PARTITION BY cnpj ORDER BY periodo ASC)  AS first_val,
    FIRST_VALUE(periodo)        OVER (PARTITION BY cnpj ORDER BY periodo ASC)  AS first_periodo,
    FIRST_VALUE(vl_saldo_final) OVER (PARTITION BY cnpj ORDER BY periodo DESC) AS last_val,
    FIRST_VALUE(periodo)        OVER (PARTITION BY cnpj ORDER BY periodo DESC) AS last_periodo
  FROM base
),
growth AS (
  SELECT DISTINCT
    cnpj,
    razao_social,
    first_val,
    last_val,
    first_periodo,
    last_periodo,
    CASE
      WHEN first_val IS NULL OR first_val = 0 THEN NULL
      ELSE ROUND(((last_val - first_val) / first_val) * 100.0, 2)
    END AS crescimento_percentual
  FROM first_last
)
SELECT
  cnpj,
  razao_social,
  first_periodo,
  last_periodo,
  first_val,
  last_val,
  crescimento_percentual
FROM growth
WHERE crescimento_percentual IS NOT NULL
ORDER BY crescimento_percentual DESC
LIMIT 5;

WITH por_operadora_uf AS (
  SELECT
    COALESCE(o.uf, 'NI') AS uf,
    d.cnpj,
    SUM(d.vl_saldo_final) AS total_operadora_uf
  FROM despesas_consolidadas d
  LEFT JOIN operadoras o
    ON o.cnpj = d.cnpj
  WHERE d.cnpj IS NOT NULL
    AND d.cnpj <> ''
  GROUP BY COALESCE(o.uf, 'NI'), d.cnpj
),
por_uf AS (
  SELECT
    uf,
    SUM(total_operadora_uf) AS total_uf,
    COUNT(*) AS qtd_operadoras,
    ROUND(AVG(total_operadora_uf), 2) AS media_por_operadora_na_uf
  FROM por_operadora_uf
  GROUP BY uf
)
SELECT
  uf,
  total_uf,
  qtd_operadoras,
  media_por_operadora_na_uf
FROM por_uf
ORDER BY total_uf DESC
LIMIT 5;

WITH base AS (
  SELECT
    d.cnpj,
    d.ano,
    d.trimestre,
    d.vl_saldo_final
  FROM despesas_consolidadas d
  WHERE d.cnpj IS NOT NULL
    AND d.cnpj <> ''
    AND (d.ano, d.trimestre) IN ((2025,1),(2025,2),(2025,3))
),
media_geral_por_trimestre AS (
  SELECT
    ano,
    trimestre,
    AVG(vl_saldo_final) AS media_geral
  FROM base
  GROUP BY ano, trimestre
),
comparacao AS (
  SELECT
    b.cnpj,
    b.ano,
    b.trimestre,
    b.vl_saldo_final,
    m.media_geral,
    CASE WHEN b.vl_saldo_final > m.media_geral THEN 1 ELSE 0 END AS acima_media
  FROM base b
  JOIN media_geral_por_trimestre m
    ON m.ano = b.ano AND m.trimestre = b.trimestre
),
contagem AS (
  SELECT
    cnpj,
    SUM(acima_media) AS qtd_trimestres_acima_media
  FROM comparacao
  GROUP BY cnpj
)
SELECT
  COUNT(*) AS operadoras_acima_media_em_pelo_menos_2_trimestres
FROM contagem
WHERE qtd_trimestres_acima_media >= 2;
