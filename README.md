# Case Técnico - Engenharia de Dados beAnalytic

Este repositório contém a solução do case técnico para a vaga de Engenheiro de Dados Pleno. O projeto implementa uma pipeline analítica utilizando Arquitetura Medalhão (Bronze, Silver, Gold) no Databricks, com foco em resiliência, idempotência e governança de dados via Unity Catalog.

O objetivo da base de dados é permitir o acompanhamento do custo do dinheiro frente à inflação, cruzando as séries históricas da taxa SELIC e do IPCA do Banco Central do Brasil.

---

## Arquitetura e Decisões Técnicas

O pipeline foi estruturado para garantir escalabilidade, rastreabilidade e facilidade de manutenção. Abaixo estão as justificativas para cada escolha arquitetural.

### 1. Extração Resiliente (Ambiente Local)
*   **Tecnologia:** Python puro (`requests` e `urllib3.util.retry`).
*   **Decisão Técnica:** Implementação de retentativas com *backoff* exponencial nativo em caso de falha de rede (HTTP 5xx).
*   **Contorno de Bloqueios:** Injeção de *headers* (User-Agent e Accept) para contornar a política de segurança antibot do Banco Central (Erro 406 Not Acceptable).
*   **Comportamento em Falhas:** Quebra de execução explícita (`raise`) caso a API retorne um *payload* vazio ou códigos de erro HTTP não tratados.

### 2. Armazenamento Bruto (Data Lake)
*   **Tecnologia:** Unity Catalog Volumes.
*   **Decisão Técnica:** Padrão atualizado do Databricks para arquivos não-tabulares (JSON), garantindo governança nativa, controle de acesso refinado e integração transparente com o motor do Spark.

### 3. Camada Bronze (Ingestão Incremental)
*   **Tecnologia:** PySpark + Databricks Auto Loader (`cloudFiles`).
*   **Decisão Técnica:** O Auto Loader gerencia a incrementalidade automaticamente via *checkpoints*, eliminando a necessidade de controle manual de estado.
*   **Otimização de Custos:** Utilização do gatilho `availableNow=True` para converter o fluxo contínuo em lote (*batch*), ligando o *compute* apenas para processar a fila pendente e desligando em seguida.
*   **Governança:** Acesso ao nome do arquivo de origem via coluna nativa `_metadata.file_path` do Unity Catalog, substituindo a função legada `input_file_name()`.

### 4. Camada Silver (Limpeza e Idempotência)
*   **Tecnologia:** PySpark + Delta Lake.
*   **Decisão Técnica:** Operação de `MERGE INTO` validando chaves primárias compostas (`data_ref` e `arquivo_origem`).
*   **Garantia de Idempotência:** A reexecução do pipeline não gera duplicação de dados. Registros existentes são atualizados e novos são inseridos. Evidências de testes de reexecução comprovando a idempotência estão na pasta `/evidencias`.
*   **Tratamento de Dados:** Tipagem padronizada para `DateType` e `DecimalType`.

### 5. Camada Gold (Regras de Negócio e Agregação)
*   **Grão da Tabela:** Consolidado Mensal (`yyyy-MM`).
*   **Tecnologia:** PySpark + Spark Window Functions.
*   **Decisão Técnica:** Tabela analítica gerada via modo `overwrite` para garantir a recálculo integral de métricas caso ocorram atualizações retroativas (*late arriving data*) na camada Silver.
*   **Matemática Financeira:** 
    *   Cálculo do Juro Real via fórmula macroeconômica: `((1 + Selic) / (1 + IPCA)) - 1`.
    *   Cálculo do IPCA acumulado de 12 meses utilizando *Window Functions* e propriedades logarítmicas `exp(sum(log(1+taxa))) - 1` para garantir a composição correta de juros em vez da simples soma linear. Uso de *OUTER JOIN* para preservar o mês corrente onde apenas a SELIC está disponível.

### 6. Orquestração e Deploy
*   **Tecnologia:** Databricks Workflows (exportado em formato YAML).
*   **Decisão Técnica:** Adoção do padrão YAML em substituição ao JSON legado, preparando o terreno para integração nativa com esteiras de CI/CD via Databricks Asset Bundles (DABs).

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
*   Instalador de pacotes moderno Python: `uv`.
*   Conta ativa no Databricks com suporte ao Unity Catalog.

### Etapa 1: Extração do Dado Bruto (Local)
1. Navegue até o diretório raiz do projeto.
2. Crie e ative um ambiente virtual com o gerenciador `uv`:
   ```bash
   uv venv
   # Ativação Windows
   .venv\Scripts\activate
   # Ativação Linux/Mac
   source .venv/bin/activate
3. Instale as dependências: 
    uv pip install -r requirements.txt
4. Execute o script de extração:
    Bash
    python extract/00_extracao_local.py
5. Faça o upload dos arquivos gerados (selic.json e ipca.json) para um Volume configurado no seu Unity Catalog no Databricks.
### Etapa 2: Execução do Pipeline (Databricks)
1. No seu Workspace do Databricks, utilize a funcionalidade Git folder para clonar este repositório e acessar os arquivos .py diretamente.

2. Certifique-se de alterar as variáveis volume_path, bronze_table, silver_table e gold_table nos scripts da pasta src/ para refletirem o seu catálogo, schema e volume correspondentes.

3. Crie um novo Job Workflow.

4. Importe a definição de tarefas através da opção "Edit as YAML", colando o conteúdo do arquivo workflow_job.yml anexo na raiz deste repositório, ou crie as três tarefas sequenciais manualmente apontando para os scripts 01_bronze, 02_silver e 03_gold.

5. Execute o Job.

### Estrutura de Pastas do Repositório:

    /extract: Scripts locais.

    /src: Códigos-fonte da pipeline em PySpark.

    /evidencias: Comprovação visual dos testes de idempotência exigidos no case.