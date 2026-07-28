from pyspark.sql.functions import col, to_date, regexp_replace
from delta.tables import DeltaTable

# 1. Definições do Unity Catalog
bronze_table = "case_beanalytic.dados_macro.bronze_bcb"
silver_table = "case_beanalytic.dados_macro.silver_bcb"

print("Iniciando processamento da Camada Silver...")

# 2. Leitura da Camada Bronze
df_bronze = spark.read.table(bronze_table)

# 3. Transformação: Tipagem e Padronização
df_clean = (df_bronze
    .withColumn("data_ref", to_date(col("data"), "dd/MM/yyyy"))
    .withColumn("valor_taxa", col("valor").cast("decimal(10,4)"))
    .drop("data", "valor", "ingestao_ts")
)

# 4. Data Quality Check (Qualidade 2 de 3)
nulos_pk = df_clean.filter(col("data_ref").isNull()).count()
if nulos_pk > 0:
    raise ValueError(f"FALHA DE DQ: Encontrados {nulos_pk} registros com 'data_ref' nula. Abortando MERGE.")

# 5. Carga Idempotente (MERGE)
try:
    delta_table = DeltaTable.forName(spark, silver_table)
    tabela_existe = True
except Exception:
    tabela_existe = False

if not tabela_existe:
    # Primeira execução: Cria a tabela
    print("Tabela Silver não existe. Criando nova tabela Delta...")
    df_clean.write.format("delta").saveAsTable(silver_table)
else:
    # Execuções subsequentes: MERGE garantindo idempotência
    print("Tabela Silver encontrada. Executando MERGE (Upsert)...")
    
    (delta_table.alias("tgt")
        .merge(
            df_clean.alias("src"),
            "tgt.data_ref = src.data_ref AND tgt.arquivo_origem = src.arquivo_origem"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

total_silver = spark.read.table(silver_table).count()
print(f"Sucesso! Tabela Silver atualizada de forma idempotente com {total_silver} registros.")