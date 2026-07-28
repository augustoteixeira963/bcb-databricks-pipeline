from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType

# 1. Definição de Caminhos (Unity Catalog)
volume_path = "/Volumes/case_beanalytic/dados_macro/raw_files/"
bronze_table = "case_beanalytic.dados_macro.bronze_bcb"
checkpoint_path = "/Volumes/case_beanalytic/dados_macro/raw_files/_checkpoints/bronze_bcb"

# 2. Definição do Schema Bruto
schema_bruto = StructType([
    StructField("data", StringType(), True),
    StructField("valor", StringType(), True)
])

print("Iniciando ingestão incremental via Auto Loader...")

# 3. Leitura Incremental (Correção Unity Catalog aplicada)
df_raw = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(schema_bruto)
    .load(volume_path)
    .withColumn("ingestao_ts", current_timestamp())
    .withColumn("arquivo_origem", col("_metadata.file_path")) # Uso da coluna oculta do UC
)

# 4. Escrita na Tabela Delta
query = (df_raw.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(bronze_table)
)

query.awaitTermination()

# 5. Data Quality Check (Qualidade 1 de 3)
total_registros = spark.read.table(bronze_table).count()

if total_registros == 0:
    raise ValueError("FALHA DE DQ: A tabela Bronze está vazia após a ingestão. Verifique os arquivos no Volume.")

print(f"Sucesso! Tabela Bronze carregada com {total_registros} registros.")