BEGIN;
SET client_encoding TO 'UTF8';

TRUNCATE TABLE despesas_consolidadas, despesas_agregadas RESTART IDENTITY;
TRUNCATE TABLE operadoras RESTART IDENTITY CASCADE;

-- Ajustar precisão das colunas se necessário
ALTER TABLE despesas_agregadas
ALTER COLUMN total_despesas TYPE DECIMAL(22,2),
ALTER COLUMN media_trimestral TYPE DECIMAL(22,2),
ALTER COLUMN desvio_padrao TYPE DECIMAL(22,2);

ALTER TABLE despesas_consolidadas
ALTER COLUMN vl_saldo_final TYPE DECIMAL(22,2);

DROP TABLE IF EXISTS operadoras_ativas_raw;
DROP TABLE IF EXISTS operadoras_canceladas_raw;

CREATE TABLE operadoras_ativas_raw (linha TEXT);
CREATE TABLE operadoras_canceladas_raw (linha TEXT);

-- ✅ AGORA LÊ DO VOLUME /shared (em vez de /tmp)
COPY operadoras_ativas_raw (linha)
FROM '/shared/Relatorio_cadop.csv'
WITH (FORMAT text, ENCODING 'LATIN1');

COPY operadoras_canceladas_raw (linha)
FROM '/shared/Relatorio_cadop_canceladas.csv'
WITH (FORMAT text, ENCODING 'LATIN1');

WITH src AS (
  SELECT 'ATIVA' AS situacao, linha FROM operadoras_ativas_raw
  UNION ALL
  SELECT 'CANCELADA' AS situacao, linha FROM operadoras_canceladas_raw
),
limpa AS (
  SELECT
    situacao,
    replace(linha, '""', '"') AS linha
  FROM src
  WHERE linha IS NOT NULL AND trim(linha) <> ''
),
dados AS (
  SELECT
    situacao,
    trim(both '"' from split_part(linha, ';', 1)) AS registro_ans,
    trim(both '"' from split_part(linha, ';', 2)) AS cnpj,
    convert_from(convert_to(trim(both '"' from split_part(linha, ';', 3)), 'LATIN1'), 'UTF8') AS razao_social,
    convert_from(convert_to(trim(both '"' from split_part(linha, ';', 5)), 'LATIN1'), 'UTF8') AS modalidade,
    convert_from(convert_to(trim(both '"' from split_part(linha, ';', 11)), 'LATIN1'), 'UTF8') AS uf
  FROM limpa
),
normalizado AS (
  SELECT
    situacao,
    NULLIF(trim(registro_ans), '') AS registro_ans,
    NULLIF(regexp_replace(cnpj, '\D', '', 'g'), '') AS cnpj,
    NULLIF(trim(razao_social), '') AS razao_social,
    NULLIF(trim(modalidade), '') AS modalidade,
    CASE
      WHEN upper(left(regexp_replace(trim(uf), '[^A-Za-z]', '', 'g'), 2)) ~ '^[A-Z]{2}$'
        THEN upper(left(regexp_replace(trim(uf), '[^A-Za-z]', '', 'g'), 2))
      ELSE NULL
    END AS uf
  FROM dados
  WHERE cnpj IS NOT NULL
    AND trim(cnpj) <> ''
    AND cnpj !~ '^(CNPJ|Cnpj)'
    AND registro_ans !~ '^(REGISTRO_OPERADORA|Registro_ANS)'
),
dedup AS (
  SELECT DISTINCT ON (cnpj)
    cnpj, registro_ans, razao_social, modalidade, uf, situacao
  FROM normalizado
  WHERE razao_social IS NOT NULL
    AND cnpj ~ '^\d+$'
    AND length(cnpj) = 14
  ORDER BY
    cnpj,
    CASE WHEN situacao = 'ATIVA' THEN 1 ELSE 2 END,
    registro_ans NULLS LAST
)
INSERT INTO operadoras (cnpj, registro_ans, razao_social, modalidade, uf, situacao)
SELECT
  cnpj, registro_ans, razao_social, modalidade, uf, situacao
FROM dedup
ON CONFLICT (cnpj) DO UPDATE
SET
  registro_ans = COALESCE(EXCLUDED.registro_ans, operadoras.registro_ans),
  razao_social = COALESCE(EXCLUDED.razao_social, operadoras.razao_social),
  modalidade = COALESCE(EXCLUDED.modalidade, operadoras.modalidade),
  uf = COALESCE(EXCLUDED.uf, operadoras.uf),
  situacao = CASE
    WHEN operadoras.situacao = 'ATIVA' THEN operadoras.situacao
    ELSE EXCLUDED.situacao
  END;

DROP TABLE IF EXISTS despesas_consolidadas_staging;
DROP TABLE IF EXISTS despesas_agregadas_staging;

CREATE TABLE despesas_consolidadas_staging (
  registro_ans_raw   TEXT,
  vl_saldo_final_raw TEXT,
  ano_raw            TEXT,
  trimestre_raw      TEXT,
  cnpj_raw           TEXT,
  razao_social_raw   TEXT
);

CREATE TABLE despesas_agregadas_staging (
  razao_social     TEXT,
  uf               TEXT,
  total_despesas   TEXT,
  media_trimestral TEXT,
  desvio_padrao    TEXT
);

-- ✅ AGORA LÊ DO VOLUME /shared (em vez de /tmp)
COPY despesas_consolidadas_staging (registro_ans_raw, vl_saldo_final_raw, ano_raw, trimestre_raw, cnpj_raw, razao_social_raw)
FROM '/shared/despesas_consolidadas_final.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ';', QUOTE '"', ENCODING 'UTF8');

COPY despesas_agregadas_staging (razao_social, uf, total_despesas, media_trimestral, desvio_padrao)
FROM '/shared/despesas_agregadas.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ';', QUOTE '"', ENCODING 'UTF8');

-- INSERIR TODAS AS OPERADORAS FALTANTES
INSERT INTO operadoras (cnpj, registro_ans, razao_social, modalidade, uf, situacao)
SELECT DISTINCT
  regexp_replace(cnpj_raw, '\D', '', 'g') AS cnpj,
  trim(registro_ans_raw) AS registro_ans,
  trim(razao_social_raw) AS razao_social,
  NULL AS modalidade,
  NULL AS uf,
  'DESCONHECIDA' AS situacao
FROM despesas_consolidadas_staging
WHERE regexp_replace(cnpj_raw, '\D', '', 'g') <> ''
  AND length(regexp_replace(cnpj_raw, '\D', '', 'g')) = 14
ON CONFLICT (cnpj) DO NOTHING;

-- INSERIR DESPESAS
INSERT INTO despesas_consolidadas (registro_ans, cnpj, razao_social, ano, trimestre, vl_saldo_final)
SELECT
  NULLIF(trim(registro_ans_raw), ''),
  regexp_replace(cnpj_raw, '\D', '', 'g'),
  NULLIF(trim(razao_social_raw), ''),
  CAST(NULLIF(trim(ano_raw), '') AS SMALLINT),
  CAST(NULLIF(trim(trimestre_raw), '') AS SMALLINT),
  CAST(replace(replace(trim(vl_saldo_final_raw), '.', ''), ',', '.') AS DECIMAL(22,2))
FROM despesas_consolidadas_staging
WHERE trim(registro_ans_raw) <> ''
  AND regexp_replace(cnpj_raw, '\D', '', 'g') <> ''
  AND length(regexp_replace(cnpj_raw, '\D', '', 'g')) = 14
  AND trim(razao_social_raw) <> ''
  AND ano_raw ~ '^\d+$'
  AND trimestre_raw ~ '^\d+$'
  AND trim(vl_saldo_final_raw) <> '';

-- INSERIR AGREGADAS
INSERT INTO despesas_agregadas (razao_social, uf, total_despesas, media_trimestral, desvio_padrao)
SELECT
  NULLIF(trim(razao_social), ''),
  CASE
    WHEN upper(left(regexp_replace(trim(uf), '[^A-Za-z]', '', 'g'), 2)) ~ '^[A-Z]{2}$'
      THEN upper(left(regexp_replace(trim(uf), '[^A-Za-z]', '', 'g'), 2))
    ELSE NULL
  END,
  CAST(NULLIF(replace(replace(trim(total_despesas), '.', ''), ',', '.'), '') AS DECIMAL(22,2)),
  CAST(NULLIF(replace(replace(trim(media_trimestral), '.', ''), ',', '.'), '') AS DECIMAL(22,2)),
  CAST(NULLIF(replace(replace(trim(desvio_padrao), '.', ''), ',', '.'), '') AS DECIMAL(22,2))
FROM despesas_agregadas_staging
WHERE NULLIF(trim(razao_social), '') IS NOT NULL
  AND NULLIF(trim(total_despesas), '') IS NOT NULL;

DROP TABLE operadoras_ativas_raw;
DROP TABLE operadoras_canceladas_raw;
DROP TABLE despesas_consolidadas_staging;
DROP TABLE despesas_agregadas_staging;

COMMIT;
