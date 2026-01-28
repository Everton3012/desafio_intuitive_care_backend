# Teste Técnico - Estágio IntuitiveCare 2026

## Visão Geral
Projeto desenvolvido em Python para coleta, processamento, validação, consolidação e
disponibilização de dados de despesas das operadoras de saúde a partir da API de Dados Abertos da ANS.

O projeto está dividido em etapas que correspondem às partes do teste técnico: ETL de dados,
validação e enriquecimento, consultas SQL e disponibilização via API.

## Tecnologias
- Python 3.10+
- Pandas
- FastAPI
- Requests
- Git

## Estrutura do Projeto

```
Teste_IntuitiveCare/
│
├── etl/                # Scripts de extração e processamento dos dados (ETL)
│   ├── download_ans.py
│   ├── process_files.py
│   └── consolidate.py
│
├── data/
│   ├── raw/            # Arquivos ZIP baixados da ANS
│   ├── extracted/     # Arquivos descompactados
│   └── final/         # CSVs finais gerados pelo processamento
│
├── api/                # API em FastAPI para disponibilização dos dados
│   └── main.py
│
├── sql/                # Scripts SQL (DDL, importação e queries analíticas)
│
├── requirements.txt
└── README.md
```

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

### 4. Fluxo geral de execução

O projeto deve ser executado seguindo o fluxo abaixo:

1. Executar os scripts de ETL para download e processamento dos dados da ANS
2. Gerar os arquivos CSV finais na pasta `data/final`
3. (Opcional) Importar os CSVs no banco de dados e executar as queries SQL
4. Executar a API para consulta e visualização dos dados

As instruções detalhadas de cada etapa serão descritas conforme os scripts forem implementados.

## Decisões Técnicas e Trade-offs

### Escolha da Linguagem
Foi escolhido Python devido à sua ampla utilização em projetos de integração e processamento
de dados, além do ecossistema de bibliotecas que facilitam a leitura de diferentes formatos
(CSV, TXT e XLSX), tratamento de dados e integração com APIs REST.

Essa escolha permite maior produtividade e foco na lógica de negócio em comparação com
abordagens mais verbosas.

### Framework de API
Para a camada de API, foi escolhido o FastAPI por ser um framework leve, com alto desempenho,
validação automática de dados e documentação integrada via Swagger, facilitando testes e uso
pelo frontend.

### Organização do Pipeline
O processo de dados foi dividido em scripts separados (download, processamento e consolidação)
para facilitar manutenção, testes e entendimento do fluxo de dados.

### Versionamento de Dados
Os arquivos brutos da ANS não são versionados no repositório por serem grandes e facilmente
reproduzíveis através dos scripts de download. Apenas o código e, opcionalmente, os arquivos
finais consolidados são versionados.

## Limitações e Melhorias Futuras

- Implementar testes automatizados para os scripts de ETL e para a API.
- Melhorar o tratamento de inconsistências nos dados, registrando logs mais detalhados.
- Persistir os dados em banco de dados relacional para consultas mais performáticas na API.
- Implementar cache para estatísticas agregadas na API.
- Criar interface frontend completa para visualização dos dados.

