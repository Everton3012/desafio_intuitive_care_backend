# Teste Técnico - Estágio IntuitiveCare 2026

## Visão Geral

Projeto desenvolvido em Python para coleta, processamento, validação, consolidação e
disponibilização de dados de despesas das operadoras de saúde a partir da API de Dados Abertos da ANS.

O projeto está dividido em etapas que correspondem às partes do teste técnico: ETL de dados,
validação e enriquecimento, consultas SQL e disponibilização via API.

---

## Fonte dos Dados

Os dados utilizados no projeto são provenientes da API de Dados Abertos da ANS (Agência Nacional de Saúde Suplementar):

https://dadosabertos.ans.gov.br/FTP/PDA/

Foram utilizados os arquivos de Demonstrações Contábeis trimestrais das operadoras de planos de saúde,
organizados por ano e trimestre, conforme estrutura disponibilizada pela ANS.

---

## Tecnologias

- Python 3.10+
- Pandas
- FastAPI
- Requests
- Git

---

## Estrutura do Projeto
```
Teste_IntuitiveCare/
│
├── etl/
│   ├── download_ans.py
│   └── process_files.py
│
├── data/
│   ├── raw/
│   ├── extracted/
│   └── final/
│
├── api/
│   └── main.py
│
├── sql/
│
├── logs/
│   └── etl.log
│
├── requirements.txt
└── README.md
```

---

## Como Executar

### 1. Criar ambiente virtual

```
python -m venv .venv
```

### 2. Ativar ambiente virtual

Windows:

```
.venv\Scripts\activate
```

Linux/Mac:

```
source .venv/bin/activate
```

### 3. Instalar dependências

```
pip install -r requirements.txt
```

---

## Execução do ETL

### 1. Download dos últimos 3 trimestres da ANS

```
python etl/download_ans.py
```

O script identifica automaticamente os últimos três trimestres disponíveis e realiza o download
dos arquivos ZIP para a pasta data/raw.

### 2. Extração, filtragem e consolidação das despesas

```
python etl/process_files.py
```

O script realiza:

- Extração automática dos arquivos ZIP
- Leitura de arquivos em diferentes formatos (CSV, TXT e XLSX)
- Filtragem de registros relacionados a despesas assistenciais
- Normalização de valores monetários
- Agregação por operadora e trimestre

O resultado é salvo em:

data/final/despesas_por_operadora_trimestre.csv

Todo o processo gera logs detalhados em:

logs/etl.log

### 3. Enriquecimento com dados cadastrais das operadoras

```
python etl/consolidate.py
```

Este script realiza o cruzamento dos dados consolidados com a base cadastral de operadoras da ANS,
adicionando CNPJ e Razão Social, gerando o arquivo final no formato exigido pelo teste técnico.

---

## Decisões Técnicas e Trade-offs

### Escolha da Linguagem

Foi escolhido Python devido à sua ampla utilização em projetos de integração e processamento de dados,
além do ecossistema de bibliotecas que facilitam a leitura de diferentes formatos de arquivos,
tratamento de dados e integração com APIs REST.

Essa escolha permite maior produtividade e foco na lógica de negócio.

### Organização do Pipeline

O processo foi dividido em scripts independentes para facilitar manutenção, testes e compreensão do
fluxo de dados:

- Download
- Processamento e consolidação
- Enriquecimento cadastral

### Uso de Memória e Performance

Os arquivos são processados individualmente e filtrados antes da agregação, evitando manter grandes
volumes de dados brutos simultaneamente em memória. Apenas os dados agregados por operadora e
trimestre são mantidos para a consolidação final.

### Tratamento de Inconsistências

Foram consideradas as seguintes situações:

- Arquivos com estruturas diferentes: apenas arquivos contendo as colunas REG_ANS, DESCRICAO e
  VL_SALDO_FINAL são processados. Arquivos fora desse padrão são ignorados e registrados em log.

- Valores zerados ou negativos: os valores são mantidos, pois podem representar ajustes contábeis ou
  estornos, sendo relevantes para análise financeira.

- Formatos inconsistentes de nomes de arquivos: quando não é possível extrair ano e trimestre do
  nome do arquivo, o registro é descartado e registrado em log.

- Divergências cadastrais: os dados de CNPJ e Razão Social são obtidos diretamente da base oficial da
  ANS, considerada como fonte de maior confiabilidade.

---

## Resultado Final

O processamento gera como saída principal:

- data/final/despesas_por_operadora_trimestre.csv  
  Contendo os valores consolidados de despesas assistenciais por operadora, ano e trimestre.

- consolidado_despesas.zip  
  Arquivo compactado contendo o CSV final no formato exigido pelo teste técnico.

---

## Limitações e Melhorias Futuras

- Implementar testes automatizados para os scripts de ETL e para a API.
- Persistir os dados em banco de dados relacional para consultas mais performáticas.
- Implementar cache para consultas frequentes na API.
- Criar interface frontend para visualização dos dados.
