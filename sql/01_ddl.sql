CREATE TABLE IF NOT EXISTS operadoras (
  cnpj             VARCHAR(14) PRIMARY KEY,
  registro_ans     VARCHAR(20),
  razao_social     TEXT NOT NULL,
  modalidade       TEXT,
  uf               CHAR(2),
  situacao         TEXT
);

CREATE TABLE IF NOT EXISTS despesas_consolidadas (
  id               BIGSERIAL PRIMARY KEY,
  registro_ans     VARCHAR(20) NOT NULL,
  cnpj             VARCHAR(14),
  razao_social     TEXT NOT NULL,
  ano              SMALLINT NOT NULL,
  trimestre        SMALLINT NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
  vl_saldo_final   DECIMAL(18,2) NOT NULL,
  CONSTRAINT fk_despesas_operadora
    FOREIGN KEY (cnpj) REFERENCES operadoras(cnpj)
);

CREATE INDEX IF NOT EXISTS idx_despesas_periodo
  ON despesas_consolidadas (ano, trimestre);

CREATE INDEX IF NOT EXISTS idx_despesas_operadora
  ON despesas_consolidadas (cnpj);

CREATE INDEX IF NOT EXISTS idx_despesas_reg_ans
  ON despesas_consolidadas (registro_ans);

CREATE TABLE IF NOT EXISTS despesas_agregadas (
  id                BIGSERIAL PRIMARY KEY,
  razao_social      TEXT NOT NULL,
  uf                CHAR(2),
  total_despesas    DECIMAL(18,2) NOT NULL,
  media_trimestral  DECIMAL(18,2),
  desvio_padrao     DECIMAL(18,2)
);

CREATE INDEX IF NOT EXISTS idx_agregadas_uf
  ON despesas_agregadas (uf);

CREATE INDEX IF NOT EXISTS idx_agregadas_total
  ON despesas_agregadas (total_despesas DESC);
