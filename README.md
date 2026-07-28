# Case Técnico - Engenharia de Dados beAnalytic

Este repositório contém a solução do case técnico para a vaga de Engenheiro de Dados Pleno. O projeto implementa uma pipeline analítica utilizando Arquitetura Medalhão (Bronze, Silver, Gold) no Databricks, com foco em resiliência, idempotência, governança de dados via Unity Catalog e automação completa via Databricks SDK e Asset Bundles (DABs).

O objetivo da base de dados é permitir o acompanhamento do custo do dinheiro frente à inflação, cruzando as séries históricas da taxa SELIC e do IPCA do Banco Central do Brasil.

---

## Arquitetura e Decisões Técnicas

O pipeline foi estruturado para garantir escalabilidade, rastreabilidade e facilidade de manutenção. Abaixo estão as justificativas para cada escolha arquitetural.

```
[API BCB (SGS)] ──(requests+retry)──> [JSONs Locais] ──(Databricks SDK)──> [Unity Catalog Volume]
                                                                                │
                                                                       (Databricks Auto Loader)
                                                                                ▼
[Gold: Tabela Mensal] <──(Overwrite/Window)── [Silver: Delta Merge] <──(Append)── [Bronze: Raw Delta]
```

### 1. Extração Resiliente (Ambiente Local)
*   **Tecnologia:** Python puro (`requests` e `urllib3.util.retry`).
*   **Decisão Técnica:** Implementação de retentativas com *backoff* exponencial nativo em caso de falha de rede (HTTP 5xx).
*   **Contorno de Bloqueios:** Injeção de *headers* (User-Agent e Accept) para contornar a política de segurança antibot do Banco Central (Erro 406 Not Acceptable).
*   **Comportamento em Falhas:** Quebra de execução explícita (`raise`) caso a API retorne um *payload* vazio ou códigos de erro HTTP não tratados.

### 2. Automação de Upload (Diferencial - Databricks SDK)
*   **Tecnologia:** `databricks-sdk` (`WorkspaceClient`).
*   **Decisão Técnica:** Script Python (`extract/01_upload_to_volume.py`) que transfere os arquivos brutos diretamente para o Volume do Unity Catalog via API Files de forma segura e programática, eliminando a dependência de uploads manuais via interface gráfica (UI).

### 3. Armazenamento Bruto (Data Lake)
*   **Tecnologia:** Unity Catalog Volumes.
*   **Decisão Técnica:** Padrão atualizado do Databricks para arquivos não-tabulares (JSON), garantindo governança nativa, controle de acesso refinado e integração transparente com o motor do Spark.

### 4. Camada Bronze (Ingestão Incremental)
*   **Tecnologia:** PySpark + Databricks Auto Loader (`cloudFiles`).
*   **Decisão Técnica:** O Auto Loader gerencia a incrementalidade automaticamente via *checkpoints*, eliminando a necessidade de controle manual de estado.
*   **Otimização de Custos:** Utilização do gatilho `availableNow=True` para converter o fluxo contínuo em lote (*batch*), ligando o *compute* apenas para processar a fila pendente e desligando em seguida.
*   **Governança:** Acesso ao nome do arquivo de origem via coluna nativa `_metadata.file_path` do Unity Catalog, substituindo a função legada `input_file_name()`.

### 5. Camada Silver (Limpeza e Idempotência)
*   **Tecnologia:** PySpark + Delta Lake.
*   **Decisão Técnica:** Operação de `MERGE INTO` validando chaves primárias compostas (`data_ref` e `arquivo_origem`).
*   **Garantia de Idempotência:** A reexecução do pipeline não gera duplicação de dados. Registros existentes são atualizados e novos são inseridos. Evidências de testes de reexecução comprovando a idempotência estão na pasta `/evidencias`.
*   **Tratamento de Dados:** Tipagem padronizada para `DateType` e `DecimalType`.

### 6. Camada Gold (Regras de Negócio e Agregação)
*   **Grão da Tabela:** Consolidado Mensal (`yyyy-MM`).
*   **Tecnologia:** PySpark + Spark Window Functions.
*   **Decisão Técnica:** Tabela analítica gerada via modo `overwrite` para garantir o recálculo integral de métricas caso ocorram atualizações retroativas (*late arriving data*) na camada Silver.
*   **Matemática Financeira:** 
    *   Cálculo do Juro Real via fórmula macroeconômica: `((1 + Selic) / (1 + IPCA)) - 1`.
    *   Cálculo do IPCA acumulado de 12 meses utilizando *Window Functions* e propriedades logarítmicas `exp(sum(log(1+taxa))) - 1` para garantir a composição correta de juros em vez da simples soma linear. Uso de *OUTER JOIN* para preservar o mês corrente onde apenas a SELIC está disponível.

### 7. Orquestração & IaC (Diferencial - Databricks Asset Bundles / DABs)
*   **Tecnologia:** Databricks Asset Bundles (`databricks.yml`) & Databricks Workflows (`workflow_job.yml`).
*   **Decisão Técnica:** O projeto aceita deploy tanto via importação de YAML no Databricks Workflows quanto via comando nativo de IaC `databricks bundle deploy`, permitindo versionamento total da infraestrutura como código e integração contínua (CI/CD).

---

## Estratégia de Backfill (Diferencial)

Para reprocessamento ou carga histórica retroativa (backfill), o pipeline adota a seguinte estratégia por camada:

1. **Camada de Extração:** O script `extract/00_extracao_local.py` aceita os parâmetros de intervalo de datas (`dataInicial` e `dataFinal`). Para realizar um backfill de anos anteriores, basta re-executar a extração alterando o intervalo desejado e disparar o upload automatizado (`01_upload_to_volume.py`).
2. **Camada Bronze:** O Auto Loader detecta os novos arquivos disponibilizados no Volume via *checkpoint* e ingested-os em modo *append* sem impactar os dados existentes.
3. **Camada Silver:** A carga é protegida pelo comando `MERGE INTO`. Caso o backfill contenha dados de datas já existentes na Silver, a operação realiza a atualização (*update*) em vez de duplicar registros, garantindo **idempotência total**.
4. **Camada Gold:** A tabela Gold é re-computada via `overwrite`. Como o grão é mensal e depende de janelas de 12 meses (para o IPCA acumulado), a sobrescrita garante que todas as métricas compostas sejam recalculadas com precisão matemática perfeita para todo o período histórico atualizado.

---

## Garantia de Qualidade de Dados (Data Quality)

Foram implementadas travas de segurança rigorosas com quebra explícita de execução (`raise ValueError`) em pontos críticos da esteira:

1.  **Fronteira Externa:** Validação de *payload* vazio no momento da extração da API do BCB.
2.  **Fronteira Bronze:** Validação volumétrica cruzada. O *job* é interrompido caso o Auto Loader carregue uma tabela Bronze vazia.
3.  **Fronteira Silver:** Prevenção de corrupção estrutural. O `MERGE` é abortado antes da execução caso existam registros com a chave primária (`data_ref`) nula.
4.  **Fronteira Gold:** Bloqueio de processamento caso a camada Silver não forneça dados para cruzamento.

---

## Passo a Passo para Reprodução

### Requisitos Prévios
*   Gerenciador de pacotes Python: `uv` ou `pip`.
*   Conta no Databricks com Unity Catalog ativado.
*   Databricks CLI instalado (opcional, para uso do DAB/SDK).

### Etapa 1: Extração Local e Upload Automático
1. Navegue até a raiz do projeto e ative seu ambiente virtual:
   ```bash
   uv venv
   # Ativação Windows:
   .venv\Scripts\activate
   # Ativação Linux/Mac:
   source .venv/bin/activate
   ```
2. Instale as dependências: 
   ```bash
   pip install -r requirements.txt
   ```
3. Execute o script de extração local:
   ```bash
   python extract/00_extracao_local.py
   ```
4. Configure as variáveis de ambiente e faça o upload automático para o Volume:
   ```bash
   # Windows (PowerShell)
   $env:DATABRICKS_HOST="https://<seu-workspace>.cloud.databricks.com"
   $env:DATABRICKS_TOKEN="dapi..."
   python extract/01_upload_to_volume.py
   ```

### Etapa 2: Deploy & Execução no Databricks

#### Opção A: Deploy via Databricks Asset Bundle (DAB - Recomendado)
```bash
# Validar o pacote de infraestrutura
databricks bundle validate

# Fazer o deploy do Workflow e código-fonte
databricks bundle deploy

# Executar a pipeline remotamente
databricks bundle run bcb_macro_pipeline_workflow
```

#### Opção B: Deploy via Interface Gráfica (UI)
1. Clone o repositório no seu Workspace Databricks via **Repos / Git folder**.
2. Crie um novo **Job Workflow**.
3. Importe a definição YAML colando o conteúdo do arquivo `workflow_job.yml`.
4. Execute o Job.

---

## Consumo Analítico: Conexão no Power BI Desktop

Para que a equipe de Analytics ou o Analista de BI conecte diretamente na tabela final consolidada (`Gold`), siga o passo a passo:

1. **Obtenha os Dados de Conexão do Databricks:**
   - No Databricks, vá em **SQL Warehouses** (ou no seu Cluster de Compute) > guia **Connection Details**.
   - Copie o **Server Hostname** (ex: `adb-xxxxxxxx.xx.azuredatabricks.net`) e o **HTTP Path** (ex: `/sql/1.0/warehouses/xxxxxxxxxxxx`).

2. **Conectar pelo Power BI Desktop:**
   - Abra o Power BI Desktop e selecione **Obter Dados (Get Data)** > **Databricks**.
   - Insira o **Server Hostname** e o **HTTP Path**.
   - Em Modo de Conectividade de Dados, escolha **DirectQuery** (para consultas em tempo real) ou **Import** (para carregar em memória).
   - Autentique-se utilizando **Personal Access Token (PAT)** ou **Azure Active Directory / OAuth**.

3. **Navegar até a Tabela Gold:**
   - No Navegador, expanda o catálogo e o schema do Unity Catalog: `main` > `default`.
   - Selecione a tabela final: `gold_bcb_macro_mensal`.
   - Clique em **Carregar** para iniciar a criação de dashboards de acompanhamento do Juro Real e IPCA Acumulado.

---

## Estrutura de Pastas do Repositório

```
├── extract/
│   ├── 00_extracao_local.py      # Extração da API do BCB com retentativas
│   ├── 01_upload_to_volume.py    # Upload automático para UC Volume via SDK
│   ├── selic.json                # Arquivo bruto gerado
│   └── ipca.json                 # Arquivo bruto gerado
├── src/
│   ├── 01_bronze_ingestion.py    # Auto Loader incremental
│   ├── 02_silver_transformation.py # Clean, Cast e MERGE Idempotente
│   └── 03_gold_aggregation.py    # Agregação mensal, Juro Real e IPCA 12M
├── evidencias/                   # Evidências em imagem da idempotência (2x execuções)
├── databricks.yml                # Manifesto IaC (Databricks Asset Bundle)
├── workflow_job.yml              # Definição do Job Workflow em YAML
├── requirements.txt              # Dependências do projeto Python
└── README.md                     # Documentação completa
```