# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Silver: bilhetes agregados (S7)
# MAGIC
# MAGIC `silver.bilhete_agregado`, grão (`mes_emissao`, `mes_viagem`, `origem`, `destino`, `tipo_servico_norm`, `tipo_gratuidade_norm`).
# MAGIC A fonte já é agregada: não há ID de bilhete. Meses são `date` (dia 1). `uf_origem`/`uf_destino` saem de 'Cidade/UF'.
# MAGIC
# MAGIC Colunas derivadas: `tipo_servico_norm` e `tipo_gratuidade_norm` (nulo vira `NAO_INFORMADO`); os originais ficam preservados.
# MAGIC
# MAGIC Flags (sinalizam, não removem):
# MAGIC - `mes_viagem_antes_emissao`: `mes_viagem < mes_emissao`.
# MAGIC - `mes_viagem_futuro`: `mes_viagem > mes_emissao + 12 meses` (limite adotado: passagens vendidas com mais de 12 meses de antecedência não são plausíveis).
# MAGIC - `grao_repetido`: o grão aparece mais de uma vez (`grao_ocorrencias` traz a contagem).
# MAGIC - `uf_invalida`: UF de origem ou destino fora das 27 UFs.
# MAGIC
# MAGIC Escrita: overwrite determinístico (derivada só da Bronze). Como o grão pode se repetir, uma chave natural não é única e um MERGE
# MAGIC por ela colapsaria as repetições; o overwrite preserva tudo e reexecutar dá o mesmo resultado.

# COMMAND ----------

import json
from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.text("data_inicio", "2025-10", "Data início (YYYY-MM)")
dbutils.widgets.text("data_fim", "2026-09", "Data fim (YYYY-MM)")
CATALOG = dbutils.widgets.get("catalog")
B = f"{CATALOG}.antt_bronze"
S = f"{CATALOG}.antt_silver"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {S}")
LIMITE_FUTURO_MESES = 12
UFS = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI",
       "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]


def mes_date(c):
    return F.expr(f"try_to_date(concat('01/', trim(`{c}`)), 'dd/MM/yyyy')")


def nao_inf(c):
    return F.coalesce(F.nullif(F.trim(F.col(c)), F.lit("")), F.lit("NAO_INFORMADO"))


# COMMAND ----------

raw = spark.table(f"{B}.bronze_bilhetes")
RX = r"^(.*)/([A-Za-z]{2})$"
b = (raw.select(
    mes_date("mes_emissao_bilhete").alias("mes_emissao"),
    mes_date("mes_viagem").alias("mes_viagem"),
    F.trim("ponto_origem_viagem").alias("origem"),
    F.trim("ponto_destino_viagem").alias("destino"),
    F.nullif(F.trim("ponto_origem_viagem"), F.lit("")).alias("_o"),
    F.col("tipo_servico"), F.col("tipo_gratuidade"),
    nao_inf("tipo_servico").alias("tipo_servico_norm"),
    nao_inf("tipo_gratuidade").alias("tipo_gratuidade_norm"),
    F.expr("try_cast(trim(quantidade_bilhetes) as int)").alias("quantidade_bilhetes"),
    F.expr("try_cast(trim(media_valor_total) as double)").alias("media_valor_total"),
    F.expr("try_cast(trim(dp_valor_total) as double)").alias("dp_valor_total"),
    F.col("_source_file").alias("arquivo_origem"))
     .drop("_o")
     .withColumn("cidade_origem", F.trim(F.regexp_extract("origem", RX, 1)))
     .withColumn("uf_origem", F.upper(F.regexp_extract("origem", RX, 2)))
     .withColumn("cidade_destino", F.trim(F.regexp_extract("destino", RX, 1)))
     .withColumn("uf_destino", F.upper(F.regexp_extract("destino", RX, 2))))

g = Window.partitionBy("mes_emissao", "mes_viagem", "origem", "destino", "tipo_servico_norm", "tipo_gratuidade_norm")
b = (b.withColumn("grao_ocorrencias", F.count("*").over(g))
     .withColumn("grao_repetido", F.col("grao_ocorrencias") > 1)
     .withColumn("mes_viagem_antes_emissao", F.col("mes_viagem") < F.col("mes_emissao"))
     .withColumn("mes_viagem_futuro", F.col("mes_viagem") > F.add_months(F.col("mes_emissao"), LIMITE_FUTURO_MESES))
     .withColumn("uf_invalida", ~(F.col("uf_origem").isin(UFS) & F.col("uf_destino").isin(UFS))))
(b.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.bilhete_agregado"))
spark.sql(f"COMMENT ON TABLE {S}.bilhete_agregado IS 'Bilhetes agregados por (mes_emissao, mes_viagem, origem, destino, tipo_servico, tipo_gratuidade); flags de plausibilidade; sem remoção de linhas'")

# COMMAND ----------

T = spark.table(f"{S}.bilhete_agregado")
n = T.count()
r = {
    "bronze_bilhetes": raw.count(), "bilhete_agregado": n,
    "soma_quantidade_bilhetes": T.agg(F.sum("quantidade_bilhetes")).collect()[0][0],
    "mes_emissao_nulo": T.where("mes_emissao is null").count(),
    "mes_viagem_nulo": T.where("mes_viagem is null").count(),
    "quantidade_nula_ou_nao_positiva": T.where("quantidade_bilhetes is null or quantidade_bilhetes <= 0").count(),
    "mes_viagem_antes_emissao": T.where("mes_viagem_antes_emissao").count(),
    "mes_viagem_futuro_gt_12m": T.where("mes_viagem_futuro").count(),
    "grao_repetido_linhas": T.where("grao_repetido").count(),
    "grao_repetido_graos_distintos": T.where("grao_repetido").select("mes_emissao", "mes_viagem", "origem", "destino", "tipo_servico_norm", "tipo_gratuidade_norm").distinct().count(),
    "uf_invalida": T.where("uf_invalida").count(),
    "uf_invalidas_exemplos": [[x[0], x[1]] for x in T.where("uf_invalida").groupBy("uf_origem", "uf_destino").count().orderBy(F.desc("count")).limit(8).collect()],
    "tipo_servico_nao_informado": T.where("tipo_servico is null").count(),
    "tipo_gratuidade_nao_informado": T.where("tipo_gratuidade is null").count(),
    "linhas_por_mes_emissao": [[str(x[0]), x[1]] for x in T.groupBy("mes_emissao").count().orderBy("mes_emissao").collect()],
    "mes_viagem_min_max": [str(x) for x in T.agg(F.min("mes_viagem"), F.max("mes_viagem")).collect()[0]],
}
print(json.dumps(r, ensure_ascii=False, indent=1, default=str))
dbutils.notebook.exit(json.dumps(r, ensure_ascii=False, default=str))
