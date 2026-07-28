from pyspark.sql.functions import col, date_format, avg, max as spark_max, round as spark_round, exp, log, sum as spark_sum, coalesce, lit
from pyspark.sql.window import Window

silver_table = "case_beanalytic.dados_macro.silver_bcb"
gold_table = "case_beanalytic.dados_macro.gold_mensal"

print("Iniciando agregações da Camada Gold...")

df_silver = spark.read.table(silver_table)

if df_silver.count() == 0:
    raise ValueError("FALHA DE DQ: Camada Silver vazia. Abortando processamento Gold.")

# 1. Separação das Séries
df_selic = df_silver.filter(col("arquivo_origem").contains("selic"))
df_ipca = df_silver.filter(col("arquivo_origem").contains("ipca"))

# 2. Agregação Mensal
df_selic_mes = (df_selic
    .withColumn("mes_ano", date_format(col("data_ref"), "yyyy-MM"))
    .groupBy("mes_ano")
    .agg(avg("valor_taxa").alias("selic_media_mes"))
)

df_ipca_mes = (df_ipca
    .withColumn("mes_ano", date_format(col("data_ref"), "yyyy-MM"))
    .groupBy("mes_ano")
    .agg(spark_max("valor_taxa").alias("ipca_mes"))
)

# 3. Join Seguro (OUTER)
df_gold = df_selic_mes.join(df_ipca_mes, on="mes_ano", how="outer")

# Tratamento de nulos (se o IPCA do mês atual ainda não saiu coloca 0 para não quebrar a conta)
df_gold = df_gold.fillna(0, subset=["selic_media_mes", "ipca_mes"])

# 4. Regras de Negócio
# Juro Real = ((1 + Selic)/(1 + IPCA)) - 1
df_gold = df_gold.withColumn(
    "juro_real_mes",
    ((1 + (col("selic_media_mes") / 100)) / (1 + (col("ipca_mes") / 100))) - 1
)

# 5. Acumulado 12 Meses (Matemática Financeira Correta via Window + Log/Exp)
window_12m = Window.orderBy("mes_ano").rowsBetween(-11, Window.currentRow)

# A matemática exige: exp(sum(log(1 + taxa/100))) - 1
df_gold = df_gold.withColumn(
    "ipca_acumulado_12m",
    exp(spark_sum(log(1 + (col("ipca_mes") / 100))).over(window_12m)) - 1
)

# Limpeza e formatação final
df_gold = (df_gold
    .withColumn("juro_real_mes", spark_round(col("juro_real_mes") * 100, 4))
    .withColumn("ipca_acumulado_12m", spark_round(col("ipca_acumulado_12m") * 100, 4))
    .withColumn("selic_media_mes", spark_round(col("selic_media_mes"), 4))
    .orderBy("mes_ano", ascending=False)
)

df_gold.write.format("delta").mode("overwrite").saveAsTable(gold_table)

print(f"Sucesso! Camada Gold consolidada com {df_gold.count()} meses.")