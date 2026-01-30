# Teste Técnico - Estágio IntuitiveCare 2026

## Visão Geral

Projeto desenvolvido em Python para coleta, processamento, consolidação, validação,
enriquecimento e agregação de dados de despesas das operadoras de saúde a partir da
API de Dados Abertos da ANS.

O projeto foi implementado como um **pipeline automatizado ponta a ponta**, cobrindo
integralmente os Testes 1, 2 e 3 do desafio técnico.

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
- FastAPI (prevista para disponibilização via API, conforme escopo do teste)
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
│   └── main.py
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

---

## Teste 3 – Banco de Dados e Análise (PostgreSQL)

Foi implementada uma camada de persistência e análise utilizando PostgreSQL,
conforme solicitado no Teste 3 do desafio técnico.

### Principais atividades realizadas:

- Criação de tabelas normalizadas (operadoras, despesas consolidadas e agregadas)
- Definição de chaves primárias, estrangeiras e índices
- Importação robusta dos CSVs utilizando tabelas de staging
- Tratamento de:
  - encoding (LATIN1 / UTF-8)
  - dados inconsistentes
  - duplicidades
  - valores nulos
- Execução de queries analíticas avançadas utilizando CTEs e window functions

Os scripts SQL estão disponíveis no diretório `sql/` e foram executados em um
container PostgreSQL via Docker.


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
- Persistir dados em banco relacional
- Evoluir a API em FastAPI
- Implementar cache para consultas frequentes
