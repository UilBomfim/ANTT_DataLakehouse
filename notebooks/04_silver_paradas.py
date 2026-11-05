# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Silver: paradas fretado (S4)
# MAGIC
# MAGIC `silver.parada_fretado`: chave natural (`codigo_viagem`, `data_parada`, `motivo_parada`).
# MAGIC - `ano_mes_parada` vem da `data_parada`, não do arquivo (o arquivo é do mês de início da viagem; 6.419 linhas caem no mês seguinte).
# MAGIC - `mes_parcial`: o mês de referência do ARQUIVO está `parcial` ou `ausente` em `silver.cobertura_fonte_mes` (fonte `paradas_fretado`).
# MAGIC - `viagem_nao_encontrada`: `codigo_viagem` não existe em `silver.viagem_fretado` (rodar depois de `04_silver_viagens`).
# MAGIC - Dedup: primeiro por `_row_hash` (linha idêntica), depois pela chave natural (fica o de maior `_file_mod_time`/`_ingest_ts`).
# MAGIC - `silver.quarentena_parada_fretado`: linhas sem `codigo_viagem`, `data_parada` válida ou `motivo_parada`.
# MAGIC
# MAGIC Escrita: overwrite determinístico (as flags dependem de `viagem_fretado` e de `cobertura_fonte_mes`, reconstruídas por inteiro).
# MAGIC Reexecutar dá o mesmo resultado, sem duplicar.

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
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")
FMT = "dd-MM-yyyy HH:mm:ss"


def so_digitos(c):
    return F.regexp_replace(c, r"\D", "")


def cnpj14(c):
    d = so_digitos(c)
    return F.when(F.length(d).between(1, 14), F.lpad(d, 14, "0"))


def licenca_id(c):
    t = F.trim(c)
    return F.when(t.rlike(r"^\d{1,18}$"), t.cast("bigint"))


def dbl_virgula(c):
    return F.expr(f"try_cast(replace(trim(`{c}`), ',', '.') as double)")


# COMMAND ----------

raw = spark.table(f"{B}.bronze_paradas_fretado")
n_bronze = raw.count()

# 1) linhas idênticas (_row_hash): fica a mais recente
wh = Window.partitionBy("_row_hash").orderBy(F.col("_file_mod_time").desc(), F.col("_ingest_ts").desc(), F.col("_source_file").desc())
sem_hash_dup = raw.withColumn("_rn", F.row_number().over(wh)).where("_rn = 1").drop("_rn")
n_pos_hash = sem_hash_dup.count()

p = (sem_hash_dup.select(
    F.nullif(F.trim("codigo_viagem"), F.lit("")).alias("codigo_viagem"),
    cnpj14(F.col("cnpj")).alias("cnpj"),
    F.nullif(F.upper(F.trim("placa")), F.lit("")).alias("placa"),
    licenca_id(F.col("autorizacao_viagem")).alias("licenca_id"),
    F.expr("try_cast(trim(motivo_parada) as int)").alias("motivo_parada"),
    F.expr(f"try_to_timestamp(data_parada, '{FMT}')").alias("data_parada"),
    dbl_virgula("latitude").alias("latitude"), dbl_virgula("longitude").alias("longitude"), dbl_virgula("pdop").alias("pdop"),
    F.expr("try_cast(trim(sentido_linha) as int)").alias("sentido_linha"),
    F.col("_source_file").alias("arquivo_origem"),
    F.concat(F.regexp_extract("_source_file", r"(\d{2})_(\d{4})\.csv$", 2), F.lit("-"),
             F.regexp_extract("_source_file", r"(\d{2})_(\d{4})\.csv$", 1)).alias("ano_mes_arquivo"),
    "_file_mod_time", "_ingest_ts", "_row_hash")
     .withColumn("ano_mes_parada", F.date_format("data_parada", "yyyy-MM")))

invalida = F.col("codigo_viagem").isNull() | F.col("data_parada").isNull() | F.col("motivo_parada").isNull()
q = p.where(invalida).withColumn("motivo", F.lit("chave_natural_nula_ou_invalida"))
p = p.where(~invalida)

# 2) chave natural
wk = Window.partitionBy("codigo_viagem", "data_parada", "motivo_parada").orderBy(F.col("_file_mod_time").desc(), F.col("_ingest_ts").desc(), F.col("arquivo_origem").desc(), F.col("_row_hash"))
p = p.withColumn("_rn", F.row_number().over(wk)).where("_rn = 1").drop("_rn")

# COMMAND ----------

cob = (spark.table(f"{S}.cobertura_fonte_mes").where("fonte = 'paradas_fretado'")
       .select(F.col("ano_mes").alias("ano_mes_arquivo"), (F.col("status").isin("parcial", "ausente")).alias("mes_parcial"),
               F.col("status").alias("status_cobertura_arquivo")))
viag = spark.table(f"{S}.viagem_fretado").select("codigo_viagem").distinct().withColumn("_v", F.lit(1))

out = (p.join(F.broadcast(cob), "ano_mes_arquivo", "left")
       .join(viag, "codigo_viagem", "left")
       .withColumn("mes_parcial", F.coalesce("mes_parcial", F.lit(False)))
       .withColumn("viagem_nao_encontrada", F.col("_v").isNull())
       .select("codigo_viagem", "data_parada", "motivo_parada", "ano_mes_parada", "cnpj", "placa", "licenca_id",
               "latitude", "longitude", "pdop", "sentido_linha", "arquivo_origem", "ano_mes_arquivo",
               "mes_parcial", "status_cobertura_arquivo", "viagem_nao_encontrada"))
(out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.parada_fretado"))
(q.select("codigo_viagem", "cnpj", "placa", "licenca_id", "motivo_parada", "data_parada", "arquivo_origem", "ano_mes_arquivo", "_row_hash", "motivo")
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.quarentena_parada_fretado"))
spark.sql(f"COMMENT ON TABLE {S}.parada_fretado IS 'Uma linha por (codigo_viagem, data_parada, motivo_parada); mês pela data_parada; mes_parcial pelo status do arquivo'")

# COMMAND ----------

P = spark.table(f"{S}.parada_fretado")
r = {
    "bronze_paradas": n_bronze,
    "descartadas_hash_identico": n_bronze - n_pos_hash,
    "quarentena_chave_nula": spark.table(f"{S}.quarentena_parada_fretado").count(),
    "descartadas_chave_natural_repetida": n_pos_hash - spark.table(f"{S}.quarentena_parada_fretado").count() - P.count(),
    "parada_fretado": P.count(),
    "duplicatas_chave_natural": P.groupBy("codigo_viagem", "data_parada", "motivo_parada").count().where("count > 1").count(),
    "mes_parcial": P.where("mes_parcial").count(),
    "viagem_nao_encontrada": P.where("viagem_nao_encontrada").count(),
    "por_ano_mes_parada": [[x[0], x[1]] for x in P.groupBy("ano_mes_parada").count().orderBy("ano_mes_parada").collect()],
    "mes_parada_diferente_do_arquivo": P.where("ano_mes_parada <> ano_mes_arquivo").count(),
    "por_status_arquivo": {str(x[0]): x[1] for x in P.groupBy("status_cobertura_arquivo").count().collect()},
    "viagem_nao_encontrada_por_arquivo": [[x[0], x[1]] for x in P.where("viagem_nao_encontrada").groupBy("ano_mes_arquivo").count().orderBy("ano_mes_arquivo").collect()],
}
print(json.dumps(r, ensure_ascii=False, indent=1, default=str))
dbutils.notebook.exit(json.dumps(r, ensure_ascii=False, default=str))
