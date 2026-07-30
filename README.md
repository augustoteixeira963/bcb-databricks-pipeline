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
*   **Verificação de Arquivos no Volume:**
    ```sql
    -- Listagem direta dos arquivos contidos no Volume do Unity Catalog
    LIST '/Volumes/case_beanalytic/dados_macro/raw_files';

    -- Consulta para verificar os arquivos de origem mapeados na tabela Bronze
    SELECT DISTINCT arquivo_origem, count(*) AS total_registros 
    FROM case_beanalytic.dados_macro.bronze_bcb 
    GROUP BY arquivo_origem;
    ```

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

## Ferramentas e Tecnologias Utilizadas

Abaixo está a relação de todas as ferramentas e tecnologias empregadas no projeto, com a justificativa técnica e a forma como foram aplicadas:

1. **Python (`requests` & `urllib3`)**:
   - **Como foi usada:** Na construção do script de extração local (`extract/00_extracao_local.py`).
   - **Por que foi usada:** Para consumir a API REST do Banco Central do Brasil com tratamento de erros de rede, retentativas com *backoff* exponencial e injeção de *headers* para prevenção do erro 406 (bloqueio antibot).

2. **Databricks SDK para Python (`databricks-sdk`)**:
   - **Como foi usada:** No script de upload automatizado (`extract/01_upload_to_volume.py`).
   - **Por que foi usada:** Para realizar a transferência programática dos arquivos brutos para o Volume do Unity Catalog via Files API, eliminando processos manuais de drag-and-drop.

3. **Databricks & Unity Catalog**:
   - **Como foi usada:** Como plataforma de Lakehouse para governança de arquivos brutos em Volumes e registro unificado das tabelas nas camadas Bronze, Silver e Gold.
   - **Por que foi usada:** Para fornecer governança centralizada, controle de acesso e ambiente Serverless Compute para execução das cargas.

4. **PySpark & Delta Lake**:
   - **Como foi usada:** No processamento de dados nas três camadas da esteira Medalhão.
   - **Por que foi usada:** O Auto Loader (`cloudFiles`) viabilizou a ingestão incremental com gerenciamento de *checkpoints*, o Delta Lake garantiu transações ACID com suporte à carga idempotente via `MERGE INTO`, e as *Window Functions* permitiram a composição matemática de juros compostos.

5. **Databricks Asset Bundles (DABs) & Databricks CLI**:
   - **Como foi usada:** No arquivo `databricks.yml` e na execução dos comandos de deploy e execução remota (`databricks bundle deploy` / `run`).
   - **Por que foi usada:** Para implementar Infraestrutura como Código (IaC), permitindo o versionamento e a automação do deploy do pipeline.

6. **Power BI Desktop**:
   - **Como foi usada:** Na camada de consumo final, conectando à tabela `gold_mensal` via conector nativo Databricks.
   - **Por que foi usada:** Para viabilizar a criação de relatórios visuais e dashboards executivos de acompanhamento da inflação e juro real.

7. **Antigravity (Google DeepMind AI Agent)**:
   - **Como foi usada:** Atuou como assistente de IA em formato de mentoria técnica e programação em par (*pair programming*) durante o desenvolvimento.
   - **Por que foi usada:** Auxiliou no desenho da arquitetura, refatoração de código no padrão PEP-8, tratamento de incompatibilidades do bundle com o ambiente Serverless, estruturação do plano de testes de Data Quality e padronização da documentação.

---

## Estratégia de Backfill (Diferencial)

Para reprocessamento ou carga histórica retroativa (backfill), o pipeline adota a seguinte estratégia por camada:

1. **Camada de Extração:** O script `extract/00_extracao_local.py` aceita os parâmetros de intervalo de datas (`dataInicial` e `dataFinal`). Para realizar um backfill de anos anteriores, basta re-executar a extração alterando o intervalo desejado e disparar o upload automatizado (`01_upload_to_volume.py`).
2. **Camada Bronze:** O Auto Loader detecta os novos arquivos disponibilizados no Volume via *checkpoint* e ingested-os em modo *append* sem impactar os dados existentes.
3. **Camada Silver:** A carga é protegida pelo comando `MERGE INTO`. Caso o backfill contenha dados de datas já existentes na Silver, a operação realiza a atualização (*update*) em vez de duplicar registros, garantindo **idempotência total**.
4. **Camada Gold:** A tabela Gold é re-computada via `overwrite`. Como o grão é mensal e depende de janelas de 12 meses (para o IPCA acumulado), a sobrescrita garante que todas as métricas compostas sejam recalculadas com precisão matemática perfeita para todo o período histórico atualizado.

---

## Checagens de Qualidade de Dados (Data Quality) e Evidências de Falha

O pipeline implementa checagens de qualidade de dados com quebra explícita de execução (`raise ValueError`) em cada camada da esteira. O descumprimento de qualquer regra interrompe imediatamente a execução do job.

### Checagem 1: Validação de Payload Vazio na Extração (Fronteira Externa)
- **Localização:** `extract/00_extracao_local.py` (linha 32)
- **Regra:** Se a API do BCB retornar um array vazio `[]`, o script interrompe o processo para impedir que arquivos inválidos sejam criados.
- **Simulação da Falha:** Passar uma URL com intervalo de datas sem registros ou mockar o retorno da API para `[]`.
- **Evidência do Erro Gerado:**
  ```text
  ValueError: Payload vazio retornado da URL: https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?...
  ```

### Checagem 2: Validação de Tabela Bronze Vazia (Fronteira Bronze -> Silver)
- **Localização:** `src/01_bronze_ingestion.py` (linha 40)
- **Regra:** Após o término da leitura pelo Auto Loader, verifica se a tabela Bronze possui ao menos 1 registro. Se a contagem for zero, a execução é abortada.
- **Simulação da Falha:** Apontar a ingestão para um Volume ou diretório vazio.
- **Evidência do Erro Gerado:**
  ```text
  ValueError: FALHA DE DQ: A tabela Bronze está vazia após a ingestão. Verifique os arquivos no Volume.
  ```

### Checagem 3: Integridade da Chave Primária na Silver (Fronteira Silver -> MERGE)
- **Localização:** `src/02_silver_transformation.py` (linha 22)
- **Regra:** Verifica a presença de valores nulos na coluna `data_ref` (chave de negócio). Se houver nulos, a instrução `MERGE INTO` é abortada antes de alterar a tabela de destino.
- **Simulação da Falha:** Inserir um registro com a data formatada incorretamente (exemplo: `"data": "invalid_date"`), o que faz a conversão `to_date` gerar um valor `NULL`.
- **Evidência do Erro Gerado:**
  ```text
  ValueError: FALHA DE DQ: Encontrados 1 registros com 'data_ref' nula. Abortando MERGE.
  ```

### Checagem 4: Disponibilidade de Dados para a Camada Gold (Fronteira Silver -> Gold)
- **Localização:** `src/03_gold_aggregation.py` (linha 11)
- **Regra:** Interrompe o processamento analítico se a camada Silver estiver zerada.
- **Simulação da Falha:** Executar o script da camada Gold antes do processamento da Silver ou com a tabela Silver truncada.
- **Evidência do Erro Gerado:**
  ```text
  ValueError: FALHA DE DQ: Camada Silver vazia. Abortando processamento Gold.
  ```

---

## Passo a Passo para Reprodução

### Requisitos Prévios
*   Gerenciador de pacotes Python: `uv` ou `pip`.
*   Conta no Databricks com Unity Catalog ativado.
*   Databricks CLI instalado (opcional, para uso do DAB/SDK).

---

### Etapa 1: Extração Local e Upload Automático

#### Como Gerar o Personal Access Token (PAT) e Definir Permissões (Scopes)
1. No workspace Databricks (navegador), acesse o perfil no canto superior direito > **Settings** (Configurações).
2. Vá em **Developer** (Desenvolvedor) > seção **Access tokens** > clique em **Manage** > **Generate new token**.
3. Na seleção de escopos (scopes), marque obrigatoriamente:
   - **`Other APIs`**: Permite acesso às APIs de Arquivos/Volumes e Databricks Asset Bundles (DABs).
   - **`BI Tools`**: Permite acesso para conexões via SQL Warehouse e Power BI.
4. Clique em **Generate** e copie o token gerado.

#### Execução do Upload
1. Ative o ambiente virtual Python:
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
3. Execute a extração local da API do BCB:
   ```bash
   python extract/00_extracao_local.py
   ```
4. Configure as variáveis de ambiente e execute o Upload Automático via SDK:
   ```bash
   # Windows (PowerShell)
   $env:DATABRICKS_HOST="https://<seu-workspace>.cloud.databricks.com"
   $env:DATABRICKS_TOKEN="dapi..."
   $env:DATABRICKS_VOLUME_PATH="/Volumes/case_beanalytic/dados_macro/raw_files"
   
   python extract/01_upload_to_volume.py
   ```

#### Solução de Problemas Conhecidos (Troubleshooting)

Abaixo estão listados os principais erros observados durante a configuração e suas respectivas correções:

1. **Erro: `Access tokens scopes must be selected`**
   - **Causa:** Nenhum escopo foi selecionado na criação do token no Databricks.
   - **Solução:** Marcar as opções **`Other APIs`** e **`BI Tools`** no momento de gerar o token.

2. **Erro: `FAILED to initialize Databricks SDK client: host is required`**
   - **Causa:** Variáveis de ambiente com a URL do workspace e token não foram declaradas no terminal ativo.
   - **Solução:** Executar a atribuição das variáveis `$env:DATABRICKS_HOST` e `$env:DATABRICKS_TOKEN` no PowerShell antes de rodar o script.

3. **Erro: `ModuleNotFoundError: No module named 'databricks'`**
   - **Causa:** O pacote `databricks-sdk` não foi instalado no ambiente virtual em execução.
   - **Solução:** Ativar o ambiente `.venv` e executar `pip install -r requirements.txt`.

4. **Erro: `Catalog 'main' does not exist`**
   - **Causa:** O caminho de destino tenta usar um catálogo inexistente no workspace.
   - **Solução:** Ajustar a variável `$env:DATABRICKS_VOLUME_PATH` para o caminho exato do volume criado no Unity Catalog (exemplo: `/Volumes/case_beanalytic/dados_macro/raw_files`).

5. **Erro: `databricks: The term 'databricks' is not recognized...`**
   - **Causa:** O executável do Databricks CLI não foi adicionado ao PATH do sistema.
   - **Solução:** Copiar o arquivo `databricks.exe` para a pasta `.venv\Scripts\` ou para `$env:LOCALAPPDATA\Microsoft\WindowsApps`.

6. **Erro: `FALHA DE DQ: A tabela Bronze está vazia após a ingestão...` ao resetar o ambiente**
   - **Causa:** O Auto Loader armazena o histórico de arquivos já lidos na pasta `_checkpoints` dentro do Volume. Apagar apenas as tabelas com `DROP TABLE` faz com que o Auto Loader ignore os arquivos já lidos anteriormente, resultando em 0 linhas ingeridas.
   - **Solução:** Ao realizar um reset completo do ambiente, remova também o diretório de checkpoints no Databricks SQL executando: `REMOVE '/Volumes/case_beanalytic/dados_macro/raw_files/_checkpoints';` (ou via Python Notebook: `dbutils.fs.rm("/Volumes/case_beanalytic/dados_macro/raw_files/_checkpoints", True)`).

---

### Etapa 2: Deploy & Execução no Databricks

#### Autenticação no CLI (Caso utilize DABs)
```bash
databricks auth login --host https://<seu-workspace>.cloud.databricks.com
```

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

Para conectar o Power BI Desktop à tabela consolidada na camada Gold:

1. **Obter dados de conexão do Databricks:**
   - No Databricks, acesse **SQL Warehouses** (ou o cluster em uso) e abra a guia **Connection Details**.
   - Copie o **Server Hostname** e o **HTTP Path**.

2. **Conectar pelo Power BI Desktop:**
   - Abra o Power BI Desktop e selecione **Obter Dados (Get Data)** > **Databricks**.
   - Preencha o **Server Hostname** e o **HTTP Path**.
   - Selecione o modo de conectividade (DirectQuery ou Import).
   - Autentique-se utilizando o **Personal Access Token (PAT)**.

3. **Navegar até a Tabela Gold:**
   - No navegador de dados, acesse o caminho: `case_beanalytic` > `dados_macro`.
   - Selecione a tabela `gold_mensal` e confirme o carregamento.

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