# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Silver: empresas (S5)
# MAGIC
# MAGIC - `silver.empresa_historico`: 1 linha por (cnpj, mês do snapshot, tipo).
# MAGIC - `silver.empresa`: 1 linha por CNPJ de 14 dígitos, com o snapshot mais recente (empate: fretamento antes de regular).
# MAGIC   Uma empresa pode estar habilitada nos dois tipos; por isso, além de `tipo_empresa` (tipo do último snapshot),
# MAGIC   há `tem_fretamento` e `tem_regular` (apareceu em algum snapshot do tipo) e `numero_taf`/`numero_tar` (último valor não nulo).
# MAGIC   A regra `empresa_nao_habilitada` da viagem usa `tem_fretamento`.
# MAGIC
# MAGIC Escrita: overwrite determinístico (full rebuild a partir da Bronze). Idempotente e mais simples que MERGE, pois as duas
# MAGIC tabelas são derivadas por completo da Bronze e não recebem escrita de outra origem.

# COMMAND ----------

import json
from pyspark.sql import functions as F
from pyspark.sql import types as T

dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.text("data_inicio", "2025-10", "Data início (YYYY-MM)")
dbutils.widgets.text("data_fim", "2026-09", "Data fim (YYYY-MM)")
CATALOG = dbutils.widgets.get("catalog")
B = f"{CATALOG}.antt_bronze"
S = f"{CATALOG}.antt_silver"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {S}")
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")


def _cnpj_ok(c):
    if not c or len(c) != 14 or not c.isdigit() or c == c[0] * 14:
        return False
    def dv(base, pesos):
        r = sum(int(d) * p for d, p in zip(base, pesos)) % 11
        return 0 if r < 2 else 11 - r
    d1 = dv(c[:12], [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    d2 = dv(c[:12] + str(d1), [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    return c[12:] == f"{d1}{d2}"


cnpj_valido_udf = F.udf(_cnpj_ok, T.BooleanType())

# COMMAND ----------

raw = spark.table(f"{B}.bronze_empresas")
dig = F.regexp_replace(F.col("cnpj"), r"\D", "")
base = (raw
        .withColumn("_dig", dig)
        .withColumn("cnpj", F.when(F.length("_dig").between(1, 14), F.lpad("_dig", 14, "0")))
        .withColumn("snapshot_mes", F.to_date(F.concat(
            F.regexp_extract("_source_file", r"(\d{2})_(\d{4})\.csv$", 2), F.lit("-"),
            F.regexp_extract("_source_file", r"(\d{2})_(\d{4})\.csv$", 1), F.lit("-01"))))
        .withColumn("vigencia", F.expr("try_to_date(vigencia, 'dd/MM/yyyy')"))
        .withColumn("razao_social", F.trim("razao_social"))
        .withColumn("numero_taf", F.nullif(F.trim("numero_taf"), F.lit("")))
        .withColumn("numero_tar", F.nullif(F.trim("numero_tar"), F.lit(""))))
sem_cnpj = base.where(F.col("cnpj").isNull()).count()
base = base.where(F.col("cnpj").isNotNull() & F.col("snapshot_mes").isNotNull())

# histórico: 1 linha por (cnpj, snapshot_mes, tipo); mais recente por _file_mod_time se houver repetição
from pyspark.sql.window import Window
wh = Window.partitionBy("cnpj", "snapshot_mes", "tipo_empresa").orderBy(F.col("_file_mod_time").desc(), F.col("_row_hash"))
hist = (base.withColumn("_rn", F.row_number().over(wh)).where("_rn = 1")
        .select("cnpj", "snapshot_mes", "tipo_empresa", "razao_social", "numero_taf", "numero_tar", "vigencia",
                F.col("_source_file").alias("arquivo_origem"), F.col("_file_mod_time"))
        .withColumn("cnpj_valido", cnpj_valido_udf("cnpj")))
(hist.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.empresa_historico"))

# COMMAND ----------

h = spark.table(f"{S}.empresa_historico")
ordem = F.struct("snapshot_mes", F.when(F.col("tipo_empresa") == "fretamento", 1).otherwise(0).alias("pref"), "_file_mod_time")
emp = (h.groupBy("cnpj").agg(
    F.max_by("razao_social", ordem).alias("razao_social"),
    F.max_by("tipo_empresa", ordem).alias("tipo_empresa"),
    F.max_by("vigencia", ordem).alias("vigencia"),
    F.max_by("numero_taf", F.when(F.col("numero_taf").isNotNull(), ordem)).alias("numero_taf"),
    F.max_by("numero_tar", F.when(F.col("numero_tar").isNotNull(), ordem)).alias("numero_tar"),
    F.max("snapshot_mes").alias("snapshot_mes"),
    F.max(F.when(F.col("tipo_empresa") == "fretamento", 1).otherwise(0)).cast("boolean").alias("tem_fretamento"),
    F.max(F.when(F.col("tipo_empresa") == "regular", 1).otherwise(0)).cast("boolean").alias("tem_regular"),
    F.count("*").alias("qtd_snapshots"),
    F.max("cnpj_valido").alias("cnpj_valido"),
))
(emp.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.empresa"))
spark.sql(f"COMMENT ON TABLE {S}.empresa IS 'Uma linha por CNPJ, snapshot mais recente; tem_fretamento/tem_regular indicam os tipos de habilitação vistos na janela'")
spark.sql(f"COMMENT ON TABLE {S}.empresa_historico IS 'Uma linha por (cnpj, mês do snapshot, tipo_empresa)'")

# COMMAND ----------

e = spark.table(f"{S}.empresa")
r = {
    "bronze_linhas": raw.count(),
    "bronze_sem_cnpj_ou_mes": sem_cnpj,
    "empresa_historico": spark.table(f"{S}.empresa_historico").count(),
    "empresa": e.count(),
    "empresa_cnpj_distintos": e.select("cnpj").distinct().count(),
    "empresa_duplicatas_cnpj": e.groupBy("cnpj").count().where("count > 1").count(),
    "cnpj_invalido": e.where(~F.col("cnpj_valido")).count(),
    "tem_fretamento": e.where("tem_fretamento").count(),
    "tem_regular": e.where("tem_regular").count(),
    "ambos": e.where("tem_fretamento and tem_regular").count(),
    "por_tipo": {x[0]: x[1] for x in e.groupBy("tipo_empresa").count().collect()},
}
print(json.dumps(r, ensure_ascii=False, indent=1))
dbutils.notebook.exit(json.dumps(r, ensure_ascii=False))
