# Contagem com filtro
Q_OPERADORAS_COUNT_FILTER = """
SELECT COUNT(*)::int AS total
FROM operadoras
WHERE cnpj LIKE %(q_like)s OR razao_social ILIKE %(q_like)s
"""

# Lista com filtro
Q_OPERADORAS_LIST_FILTER = """
SELECT cnpj, registro_ans, razao_social, modalidade, uf, situacao
FROM operadoras
WHERE cnpj LIKE %(q_like)s OR razao_social ILIKE %(q_like)s
ORDER BY razao_social
LIMIT %(limit)s OFFSET %(offset)s
"""

# Contagem sem filtro
Q_OPERADORAS_COUNT_ALL = """
SELECT COUNT(*)::int AS total FROM operadoras
"""

# Lista sem filtro
Q_OPERADORAS_LIST_ALL = """
SELECT cnpj, registro_ans, razao_social, modalidade, uf, situacao
FROM operadoras
ORDER BY razao_social
LIMIT %(limit)s OFFSET %(offset)s
"""

# Detalhes de uma operadora
Q_OPERADORA_DETAIL = """
SELECT cnpj, registro_ans, razao_social, modalidade, uf, situacao
FROM operadoras
WHERE cnpj = %(cnpj)s
"""

# Despesas de uma operadora
Q_OPERADORA_DESPESAS = """
SELECT ano, trimestre, vl_saldo_final
FROM despesas_consolidadas
WHERE cnpj = %(cnpj)s
ORDER BY ano, trimestre
"""

# Estatísticas gerais
Q_ESTATS = """
SELECT
  COALESCE(SUM(vl_saldo_final), 0)::numeric AS total,
  COALESCE(AVG(vl_saldo_final), 0)::numeric AS media
FROM despesas_consolidadas
"""

# Top 5 operadoras por despesas
Q_TOP5 = """
SELECT
  d.cnpj,
  MAX(o.razao_social) AS razao_social,
  SUM(d.vl_saldo_final)::numeric AS total_despesas
FROM despesas_consolidadas d
LEFT JOIN operadoras o ON o.cnpj = d.cnpj
GROUP BY d.cnpj
ORDER BY total_despesas DESC
LIMIT 5
"""

# Top 5 UFs por despesas
Q_UF_TOP5 = """
SELECT
  COALESCE(o.uf, 'NI') AS uf,
  SUM(d.vl_saldo_final)::numeric AS total_despesas
FROM despesas_consolidadas d
LEFT JOIN operadoras o ON o.cnpj = d.cnpj
GROUP BY COALESCE(o.uf, 'NI')
ORDER BY total_despesas DESC
LIMIT 5
"""

# Com q + situacao
Q_OPERADORAS_COUNT_FILTER_SITUACAO = """
SELECT COUNT(*)::int AS total
FROM operadoras
WHERE (cnpj ILIKE %(q_like)s OR razao_social ILIKE %(q_like)s)
  AND situacao = %(situacao)s;
"""

Q_OPERADORAS_LIST_FILTER_SITUACAO = """
SELECT cnpj, registro_ans, razao_social, modalidade, uf, situacao
FROM operadoras
WHERE (cnpj ILIKE %(q_like)s OR razao_social ILIKE %(q_like)s)
  AND situacao = %(situacao)s
ORDER BY razao_social
LIMIT %(limit)s OFFSET %(offset)s;
"""

# Sem q, só situacao
Q_OPERADORAS_COUNT_ALL_SITUACAO = """
SELECT COUNT(*)::int AS total
FROM operadoras
WHERE situacao = %(situacao)s;
"""

Q_OPERADORAS_LIST_ALL_SITUACAO = """
SELECT cnpj, registro_ans, razao_social, modalidade, uf, situacao
FROM operadoras
WHERE situacao = %(situacao)s
ORDER BY razao_social
LIMIT %(limit)s OFFSET %(offset)s;
"""
