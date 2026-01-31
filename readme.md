# Teste Técnico - Estágio IntuitiveCare 2026

## Visão Geral

Projeto desenvolvido em Python para coleta, processamento, consolidação, validação,
enriquecimento e agregação de dados de despesas das operadoras de saúde a partir da
API de Dados Abertos da ANS.

O projeto foi implementado como um **pipeline automatizado ponta a ponta**, cobrindo integralmente os Testes 1, 2, 3 e 4 do desafio técnico.

---

## Fonte dos Dados

Os dados utilizados no projeto são provenientes da base pública da ANS
(Agência Nacional de Saúde Suplementar):

https://dadosabertos.ans.gov.br/FTP/PDA/

Fontes utilizadas:

- Demonstrações Contábeis Trimestrais (últimos 3 trimestres disponíveis)
- Cadastro de Operadoras de Planos de Saúde **Ativas**
- Cadastro de Operadoras de Planos de Saúde **Canceladas**

---

## Tecnologias

- Python 3.10+
- Pandas
- Requests
- BeautifulSoup4
- FastAPI
- Git

---

## Estrutura do Projeto

```
Teste_IntuitiveCare/
│
├── etl/
│   ├── download_ans.py
│   ├── download_operadoras.py
│   ├── process_files.py
│   ├── consolidate.py
│   └── validate_and_aggregate.py
│
├── data/
│   ├── raw/
│   ├── extracted/
│   └── final/
│
├── logs/
│   ├── etl.log
│   └── validation.log
│
├── api/
│   ├── main.py
│   ├── db.py
│   ├── queries.py
│   └── schemas.py
│
├── sql/
│
├── run_pipeline.py
├── requirements.txt
└── README.md
```

---

## Como Executar

### 1. Criar ambiente virtual

```bash
python -m venv .venv
```

### 2. Ativar ambiente virtual

Windows:
```bash
.venv\Scripts\activate
```

Linux/Mac:
```bash
source .venv/bin/activate
```

### 3. Instalar dependências

```bash
pip install -r requirements.txt
```

---

## Execução do Pipeline Completo (Recomendado)

```bash
python run_pipeline.py
```

Esse comando executa automaticamente:

1. Download dos cadastros de operadoras (ativas e canceladas)
2. Download dos últimos 3 arquivos trimestrais de demonstrações contábeis
3. Extração e processamento dos arquivos
4. Consolidação com dados cadastrais
5. Validação, enriquecimento e agregação final
6. Geração do arquivo ZIP final exigido no teste

---

## Execução por Etapas (Opcional)

### Download das Demonstrações Contábeis

```bash
python etl/download_ans.py
```

### Download do Cadastro de Operadoras

```bash
python etl/download_operadoras.py
```
### Processamento e Consolidação Inicial (ETL)

```bash
python etl/process_files.py
```

Gera:
```
data/final/despesas_por_operadora_trimestre.csv
```

### Consolidação com Dados Cadastrais

```bash
python etl/consolidate.py
```

Gera:
```
data/final/despesas_consolidadas_final.csv
data/final/consolidado_despesas.zip
```

### Validação, Enriquecimento e Agregação (Teste 2)

```bash
python etl/validate_and_aggregate.py
```

Gera:
```
data/final/despesas_agregadas.csv
Teste_Everton_Brandao.zip
```

---

## Executar Banco (PostgreSQL via Docker) + Importar CSVs

> Pré-requisito: Docker instalado.

### 1) Subir o container PostgreSQL

Exemplo:

```bash
docker run --name intuitivecare_postgres -e POSTGRES_DB=intuitivecare -e POSTGRES_USER=intuitive -e POSTGRES_PASSWORD=intuitive123 -p 5432:5432 -d postgres:15
```
### 2) Copiar CSVs gerados para dentro do container

```bash
docker cp data/final/despesas_consolidadas_final.csv intuitivecare_postgres:/tmp/despesas_consolidadas_final.csv
docker cp data/final/despesas_agregadas.csv intuitivecare_postgres:/tmp/despesas_agregadas.csv
docker cp data/raw/Relatorio_cadop.csv intuitivecare_postgres:/tmp/Relatorio_cadop.csv
docker cp data/raw/Relatorio_cadop_canceladas.csv intuitivecare_postgres:/tmp/Relatorio_cadop_canceladas.csv
```

### 3) Rodar DDL e Import
```bash
docker exec -i intuitivecare_postgres psql -U intuitive -d intuitivecare -v ON_ERROR_STOP=1 < sql/01_ddl.sql
docker exec -i intuitivecare_postgres psql -U intuitive -d intuitivecare -v ON_ERROR_STOP=1 < sql/02_import.sql
```
### 4) Validar carga

```bash
docker exec -it intuitivecare_postgres psql -U intuitive -d intuitivecare -c "SELECT COUNT(*) operadoras FROM operadoras; SELECT COUNT(*) despesas FROM despesas_consolidadas; SELECT COUNT(*) agregadas FROM despesas_agregadas;"
```

---

## Executar API (FastAPI)

### 1) Criar arquivo `.env` na raiz

Use o `.env.example` como base e crie um `.env` com:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=intuitivecare
DB_USER=intuitive
DB_PASSWORD=intuitive123
```

### 2) Subir o servidor

```bash
uvicorn api.main:app --host 127.0.0.1 --port 8000 --reload
```

### 3) Acessar documentação (Swagger)

- http://127.0.0.1:8000/docs

### 4) Teste rápido

```bash
curl -X GET "http://127.0.0.1:8000/health"
curl -X GET "http://127.0.0.1:8000/api/operadoras?page=1&limit=10"
curl -X GET "http://127.0.0.1:8000/api/estatisticas"
```

---

## Teste 2 – Transformação e Validação de Dados

### Validações Implementadas

- CNPJ válido (formato + dígitos verificadores)
- Razão Social não vazia
- Valores numéricos não negativos

#### Estratégia para CNPJs Inválidos

**Decisão adotada:** descarte dos registros com CNPJ inválido.

**Prós:**
- Evita inconsistências no enriquecimento
- Garante integridade das agregações
- Simplifica o pipeline

**Contras:**
- Redução do volume final de dados
- Possível perda de registros com erro de origem

---

### Enriquecimento de Dados

- Join realizado via `CNPJ`
- Cadastros de operadoras ativas e canceladas são unificados
- Registros sem correspondência no cadastro são mantidos com campos nulos
- Duplicidades de CNPJ são resolvidas via `drop_duplicates`

Colunas adicionadas:
- RegistroANS
- Modalidade
- UF

---

### Agregações Realizadas

Agrupamento por:
- RazaoSocial
- UF

Métricas calculadas:
- Total de despesas
- Média trimestral
- Desvio padrão

Ordenação:
- Total de despesas (ordem decrescente)

---

## Teste 3 – Banco de Dados e Análise (PostgreSQL)

### Modelagem e Normalização

Foi adotada uma abordagem de **modelagem normalizada**, com tabelas separadas para:

- Operadoras
- Despesas consolidadas
- Despesas agregadas

Essa decisão permite evitar redundância de dados cadastrais, manter integridade
referencial e facilitar análises futuras com menor risco de inconsistências.


### Trade-offs Técnicos – Normalização

**Abordagem escolhida:** Tabelas normalizadas (Opção B)

**Justificativa:**

- O volume de dados é moderado e adequado para normalização sem impacto negativo
  relevante de performance.
- A frequência de atualização dos dados é baixa (processamento periódico),
  reduzindo o custo de joins.
- Queries analíticas se tornam mais legíveis e fáceis de manter.
- Evita duplicação de informações cadastrais das operadoras.

**Alternativa considerada (tabela desnormalizada):**
Foi descartada por aumentar redundância e dificultar a manutenção dos dados
cadastrais ao longo do tempo.

### Tipos de Dados

**Valores monetários:**
Foi utilizado o tipo `DECIMAL` em vez de `FLOAT`, garantindo precisão nos cálculos
financeiros e evitando erros de arredondamento.

**Datas e períodos:**
- Ano e trimestre foram armazenados separadamente (`SMALLINT`), pois:
  - Facilitam filtros e agregações
  - Evitam parsing de strings
  - São suficientes para o nível de granularidade do problema

**CNPJ e UF:**
- `VARCHAR` para CNPJ, preservando zeros à esquerda
- `CHAR(2)` para UF, garantindo padronização

### Importação e Tratamento de Inconsistências

A importação dos dados foi realizada utilizando **tabelas de staging**, permitindo
tratamento prévio antes da inserção nas tabelas finais.

Durante o processo, foram tratadas as seguintes situações:

- **Encoding:**  
  Arquivos das operadoras utilizam `LATIN1`, enquanto os arquivos de despesas
  utilizam `UTF-8`. O encoding foi tratado explicitamente durante o `COPY`.

- **Valores NULL em campos obrigatórios:**  
  Registros sem CNPJ, Razão Social ou valores numéricos válidos foram descartados.

- **Strings em campos numéricos:**  
  Valores monetários foram normalizados removendo separadores de milhar e
  convertidos para `DECIMAL`.

- **Duplicidade de CNPJ:**  
  Foi priorizada a operadora ativa em caso de duplicidade, mantendo apenas um
  registro por CNPJ.

Essa abordagem garante maior robustez e evita falhas durante a carga.

### Queries Analíticas

Foram desenvolvidas queries analíticas utilizando **CTEs** e **window functions**
para maior clareza e desempenho.

**Query 1 – Crescimento percentual de despesas:**
- Calcula o crescimento entre o primeiro e o último trimestre disponível por
  operadora.
- Operadoras sem dados completos são mantidas, mas excluídas do ranking caso não
  seja possível calcular o crescimento.

**Query 2 – Distribuição de despesas por UF:**
- Soma total de despesas por UF
- Cálculo da média de despesas por operadora em cada estado

**Query 3 – Operadoras acima da média:**
- Identifica operadoras com despesas acima da média geral em pelo menos 2 dos 3
  trimestres analisados.
- Foi escolhida uma abordagem baseada em agregações e CTEs por oferecer boa
  legibilidade e facilidade de manutenção.

---

## Atualização dos dados (novos trimestres)

O pipeline sempre baixa automaticamente os **últimos 3 trimestres disponíveis** na ANS.
Quando um novo trimestre é publicado, basta executar novamente:

```bash
python run_pipeline.py
```
Depois, reimportar os CSVs no PostgreSQL:

```bash
docker exec -i intuitivecare_postgres psql -U intuitive -d intuitivecare -v ON_ERROR_STOP=1 < sql/02_import.sql
```
A estratégia adotada no import é `TRUNCATE + INSERT`, garantindo consistência e simplicidade (KISS),
já que o volume é moderado.

---

## Teste 4 – API (FastAPI)

Foi implementada uma API em FastAPI para consulta das operadoras e despesas consolidadas
a partir do banco PostgreSQL gerado no Teste 3.

### Rotas implementadas

- `GET /api/operadoras`  
  Lista operadoras com paginação (`page`, `limit`) e filtro opcional `q` (CNPJ ou Razão Social).

- `GET /api/operadoras/{cnpj}`  
  Retorna detalhes de uma operadora específica.

- `GET /api/operadoras/{cnpj}/despesas`  
  Retorna o histórico de despesas da operadora nos 3 trimestres analisados.

- `GET /api/estatisticas`  
  Retorna estatísticas agregadas: total, média, top 5 operadoras e top 5 UFs por despesas.

- `GET /health`  
  Healthcheck simples com verificação de conexão ao banco.

### Trade-offs Técnicos (Backend)

**Framework:** FastAPI  
Escolhi FastAPI por oferecer tipagem, validação automática (Pydantic), Swagger/OpenAPI nativo,
boa performance e facilidade de manutenção.

**Paginação:** Offset-based (`page/limit` com `OFFSET/LIMIT`)  
Escolhida por simplicidade (KISS) e por o volume ser baixo/moderado (~4k operadoras).  
Para grandes volumes, seria melhor Keyset/Cursor pagination.

**/api/estatisticas:** queries diretas  
As estatísticas são calculadas via SQL no momento da requisição, pois os dados mudam apenas
quando o pipeline é executado. Em cenário real, poderia ser cacheado por X minutos ou pré-calculado.

**Resposta de paginação:** dados + metadados  
Retorna `{ data, total, page, limit }` para facilitar o frontend e evitar chamadas extras.

## Logs

- `logs/etl.log`: download, extração, processamento e consolidação
- `logs/validation.log`: validações, enriquecimento e agregações

---

## Resultado Final

Arquivos gerados:

- `despesas_por_operadora_trimestre.csv`
- `despesas_consolidadas_final.csv`
- `despesas_agregadas.csv`
- `Teste_Everton_Brandao.zip`

---

## Limitações e Melhorias Futuras

- Implementar testes automatizados
- Automatizar a carga no banco após a execução do pipeline (ex.: script único ou docker-compose)

---

## Frontend (Vue.js) – Status

A interface web em Vue.js está planejada conforme especificação do Teste 4, com:
- tabela paginada de operadoras
- busca por CNPJ/Razão Social
- gráfico de distribuição de despesas por UF
- página de detalhes com histórico de despesas

(Implementação em andamento / não incluída nesta entrega.)
