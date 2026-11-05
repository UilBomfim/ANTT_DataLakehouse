# Databricks notebook source
# MAGIC %md
# MAGIC # 97 - Diagnóstico: os meses "faltantes" estão dentro de arquivos de outros meses?
# MAGIC Compara o mês de referência dos registros (data de início da viagem / data da parada) com o arquivo de origem.

# COMMAND ----------

import json
from pyspark.sql import functions as F

CAT = "workspace.antt_bronze"


def por_arquivo_e_mes(tabela, col_data):
    df = spark.table(f"{CAT}.{tabela}")
    df = df.withColumn("_arq", F.regexp_extract("_source_file", r"([^/]+)$", 1))
    df = df.withColumn("_dt", F.to_timestamp(F.col(col_data), "dd-MM-yyyy HH:mm:ss"))
    df = df.withColumn("_mes_ref", F.date_format("_dt", "yyyy-MM"))
    return df


res = {}

# COMMAND ----------

# Viagens: distribuição do mês de início por arquivo
v = por_arquivo_e_mes("bronze_viagens_fretado", "data_inicio_viagem")
res["viagens_arquivo_x_mes"] = [r.asDict() for r in
    v.groupBy("_arq", "_mes_ref").count().orderBy("_arq", "_mes_ref").collect()]
res["viagens_dt_nula"] = v.filter("_dt is null").count()
res["viagens_mes_ref_total"] = [r.asDict() for r in
    v.groupBy("_mes_ref").count().orderBy("_mes_ref").collect()]

# COMMAND ----------

# Paradas: mesmo teste com a data da parada
p = por_arquivo_e_mes("bronze_paradas_fretado", "data_parada")
res["paradas_arquivo_x_mes"] = [r.asDict() for r in
    p.groupBy("_arq", "_mes_ref").count().orderBy("_arq", "_mes_ref").collect()]
res["paradas_dt_nula"] = p.filter("_dt is null").count()
res["paradas_mes_ref_total"] = [r.asDict() for r in
    p.groupBy("_mes_ref").count().orderBy("_mes_ref").collect()]

# COMMAND ----------

# Faixa de codigo_viagem por arquivo (códigos crescem com o tempo; buraco indica mês não enviado)
v2 = v.withColumn("_cod", F.col("codigo_viagem").cast("long"))
res["viagens_codigo_por_arquivo"] = [r.asDict() for r in
    v2.groupBy("_arq").agg(F.min("_cod").alias("min"), F.max("_cod").alias("max"),
                           F.countDistinct("_cod").alias("distintos"), F.count("*").alias("linhas"))
      .orderBy("_arq").collect()]

# Viagens de fev/2026 existem em algum lugar? E os códigos entre jan e mar?
res["viagens_fev2026_total"] = v.filter("_mes_ref = '2026-02'").count()
res["paradas_fev2026_total"] = p.filter("_mes_ref = '2026-02'").count()

# Paradas de ago/2026: o arquivo de agosto tem poucas linhas; as paradas de ago aparecem em outro arquivo?
res["paradas_ago2026_total"] = p.filter("_mes_ref = '2026-08'").count()
res["viagens_ago2026_total"] = v.filter("_mes_ref = '2026-08'").count()

# Duplicidade entre arquivos (mesmo codigo_viagem em arquivos diferentes)
dup = v2.groupBy("_cod").agg(F.countDistinct("_arq").alias("arqs")).filter("arqs > 1")
res["viagens_codigos_em_mais_de_um_arquivo"] = dup.count()

# Viagens com início bem anterior ao mês do arquivo (retroativas)
v3 = (v.withColumn("_mes_arq", F.regexp_extract("_arq", r"(\d{2})_(\d{4})", 0))
        .withColumn("_ano_arq", F.regexp_extract("_arq", r"(\d{2})_(\d{4})", 2).cast("int"))
        .withColumn("_m_arq", F.regexp_extract("_arq", r"(\d{2})_(\d{4})", 1).cast("int"))
        .withColumn("_dif_meses", (F.year("_dt") * 12 + F.month("_dt")) - (F.col("_ano_arq") * 12 + F.col("_m_arq"))))
res["viagens_defasagem_meses"] = [r.asDict() for r in
    v3.groupBy("_dif_meses").count().orderBy("_dif_meses").collect()]

p3 = (p.withColumn("_ano_arq", F.regexp_extract("_source_file", r"(\d{2})_(\d{4})", 2).cast("int"))
        .withColumn("_m_arq", F.regexp_extract("_source_file", r"(\d{2})_(\d{4})", 1).cast("int"))
        .withColumn("_dif_meses", (F.year("_dt") * 12 + F.month("_dt")) - (F.col("_ano_arq") * 12 + F.col("_m_arq"))))
res["paradas_defasagem_meses"] = [r.asDict() for r in
    p3.groupBy("_dif_meses").count().orderBy("_dif_meses").collect()]

# COMMAND ----------

dbutils.notebook.exit(json.dumps(res, ensure_ascii=False, default=str))
