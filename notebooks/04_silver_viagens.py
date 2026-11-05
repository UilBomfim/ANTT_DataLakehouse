# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Silver: viagens fretado (S3)
# MAGIC
# MAGIC - `silver.viagem_fretado`: chave natural `codigo_viagem` (1 linha por viagem), com `dq_flags array<string>`.
# MAGIC - `silver.quarentena_viagem_fretado`: linhas fora da tabela principal, com `motivo`:
# MAGIC   `codigo_repetido_entre_arquivos` (cópias descartadas), `codigo_repetido_mesmo_arquivo`, `fim_antes_do_inicio`, `campo_obrigatorio_nulo`.
# MAGIC - Flags (não removem): `duracao_atipica` (<1 min ou >30 dias), `empresa_nao_habilitada`, `sem_licenca`, `codigo_repetido_entre_arquivos`.
# MAGIC - Mede a **taxa de ligação viagem -> licença** (total e por mês).
# MAGIC
# MAGIC Dependências: `silver.empresa` e `silver.licenca` (rodar antes `04_silver_empresas` e `04_silver_licencas`).
# MAGIC
# MAGIC Escrita: overwrite determinístico. As flags dependem de `empresa` e `licenca`, que também são reconstruídas por inteiro,
# MAGIC então um MERGE só pela chave natural deixaria flags antigas. Reexecutar dá o mesmo resultado, sem duplicar.

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
MIN_DUR, MAX_DUR = 1.0, 30 * 24 * 60.0


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


def ano_mes_arquivo():
    return F.concat(F.regexp_extract("_source_file", r"(\d{2})_(\d{4})\.csv$", 2), F.lit("-"),
                    F.regexp_extract("_source_file", r"(\d{2})_(\d{4})\.csv$", 1))


# COMMAND ----------

raw = spark.table(f"{B}.bronze_viagens_fretado")
v = (raw.select(
    F.nullif(F.trim("codigo_viagem"), F.lit("")).alias("codigo_viagem"),
    cnpj14(F.col("cnpj")).alias("cnpj"),
    F.nullif(F.upper(F.trim("placa")), F.lit("")).alias("placa"),
    licenca_id(F.col("autorizacao_viagem")).alias("licenca_id"),
    F.expr(f"try_to_timestamp(data_inicio_viagem, '{FMT}')").alias("data_inicio"),
    F.expr(f"try_to_timestamp(data_fim_viagem, '{FMT}')").alias("data_fim"),
    F.expr("try_cast(trim(sentido_linha) as int)").alias("sentido_linha"),
    F.when(F.upper(F.trim("in_transbordo")) == "SIM", F.lit(True))
     .when(F.translate(F.upper(F.trim("in_transbordo")), "Ã", "A") == "NAO", F.lit(False)).alias("in_transbordo"),
    F.nullif(F.trim("codigo_viagem_transbordo"), F.lit("")).alias("codigo_viagem_transbordo"),
    dbl_virgula("latitude").alias("latitude"),
    dbl_virgula("longitude").alias("longitude"),
    dbl_virgula("pdop").alias("pdop"),
    F.col("_source_file").alias("arquivo_origem"),
    ano_mes_arquivo().alias("ano_mes_arquivo"),
    "_file_mod_time", "_ingest_ts", "_row_hash")
     .withColumn("duracao_min", (F.col("data_fim").cast("long") - F.col("data_inicio").cast("long")) / 60.0)
     .withColumn("ano_mes_inicio", F.date_format("data_inicio", "yyyy-MM")))

# completude: campos obrigatórios
obrig_nulo = F.col("codigo_viagem").isNull() | F.col("cnpj").isNull() | F.col("data_inicio").isNull()
q_obrig = v.where(obrig_nulo).withColumn("motivo", F.lit("campo_obrigatorio_nulo"))
v = v.where(~obrig_nulo)

# duplicidade de codigo_viagem: mantém o de maior _file_mod_time, depois _ingest_ts, depois arquivo (determinístico)
w = Window.partitionBy("codigo_viagem").orderBy(F.col("_file_mod_time").desc(), F.col("_ingest_ts").desc(),
                                                F.col("arquivo_origem").desc(), F.col("_row_hash"))
v = (v.withColumn("_rn", F.row_number().over(w))
     .withColumn("_n_copias", F.count("*").over(Window.partitionBy("codigo_viagem")))
     .withColumn("_n_arquivos", F.size(F.collect_set("arquivo_origem").over(Window.partitionBy("codigo_viagem")))))
mantido = v.where("_rn = 1")
descartado = v.where("_rn > 1")
q_dup = (descartado
         .withColumn("motivo", F.when(F.col("_n_arquivos") > 1, F.lit("codigo_repetido_entre_arquivos"))
                     .otherwise(F.lit("codigo_repetido_mesmo_arquivo"))))
mant_arq = mantido.select("codigo_viagem", F.col("arquivo_origem").alias("arquivo_mantido"))
q_dup = q_dup.join(mant_arq, "codigo_viagem", "left")

# consistência: fim antes do início => quarentena (aplicado ao registro mantido)
fim_antes = F.col("data_fim").isNotNull() & (F.col("data_fim") < F.col("data_inicio"))
q_fim = mantido.where(fim_antes).withColumn("motivo", F.lit("fim_antes_do_inicio"))
ok = mantido.where(~fim_antes)

# COMMAND ----------

emp = spark.table(f"{S}.empresa").where("tem_fretamento").select("cnpj").distinct().withColumn("_emp", F.lit(1))
lic_ids = spark.table(f"{S}.licenca").select("licenca_id").distinct().withColumn("_lic", F.lit(1))

fl = (ok.join(F.broadcast(emp), "cnpj", "left").join(lic_ids, "licenca_id", "left"))
flags = F.filter(F.array(
    F.when(F.col("duracao_min").isNotNull() & ((F.col("duracao_min") < MIN_DUR) | (F.col("duracao_min") > MAX_DUR)), F.lit("duracao_atipica")),
    F.when(F.col("_emp").isNull(), F.lit("empresa_nao_habilitada")),
    F.when(F.col("_lic").isNull(), F.lit("sem_licenca")),
    F.when(F.col("_n_arquivos") > 1, F.lit("codigo_repetido_entre_arquivos")),
    F.when(F.col("data_fim").isNull(), F.lit("sem_data_fim")),
), lambda x: x.isNotNull())

vf = fl.select("codigo_viagem", "cnpj", "placa", "licenca_id", "data_inicio", "data_fim", "duracao_min", "ano_mes_inicio",
               "ano_mes_arquivo", "sentido_linha", "in_transbordo", "codigo_viagem_transbordo", "latitude", "longitude", "pdop",
               "arquivo_origem", flags.alias("dq_flags"))
(vf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.viagem_fretado"))

qcols = ["codigo_viagem", "cnpj", "placa", "licenca_id", "data_inicio", "data_fim", "duracao_min", "ano_mes_inicio",
         "ano_mes_arquivo", "sentido_linha", "in_transbordo", "codigo_viagem_transbordo", "latitude", "longitude", "pdop",
         "arquivo_origem", "_file_mod_time", "_ingest_ts", "_row_hash", "motivo"]
q = (q_obrig.select(*qcols, F.lit(None).cast("string").alias("arquivo_mantido"))
     .unionByName(q_dup.select(*qcols, "arquivo_mantido"))
     .unionByName(q_fim.select(*qcols, F.lit(None).cast("string").alias("arquivo_mantido"))))
(q.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.quarentena_viagem_fretado"))
spark.sql(f"COMMENT ON TABLE {S}.viagem_fretado IS 'Uma linha por codigo_viagem; dq_flags sinaliza duracao_atipica, empresa_nao_habilitada, sem_licenca, codigo_repetido_entre_arquivos'")
spark.sql(f"COMMENT ON TABLE {S}.quarentena_viagem_fretado IS 'Viagens fora de viagem_fretado: duplicadas descartadas, fim antes do início, campo obrigatório nulo'")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Investigação: codigo_viagem repetido entre arquivos

# COMMAND ----------

VF = spark.table(f"{S}.viagem_fretado")
Q = spark.table(f"{S}.quarentena_viagem_fretado")

# todas as cópias (mantida + descartadas) dos códigos repetidos
dup_codes = Q.where("motivo like 'codigo_repetido%'").select("codigo_viagem").distinct()
copias = (VF.join(dup_codes, "codigo_viagem").select("codigo_viagem", "cnpj", "placa", "licenca_id", "data_inicio", "data_fim", "arquivo_origem", "ano_mes_arquivo", "ano_mes_inicio", "in_transbordo", "latitude", "longitude", "sentido_linha")
          .unionByName(Q.where("motivo like 'codigo_repetido%'").select("codigo_viagem", "cnpj", "placa", "licenca_id", "data_inicio", "data_fim", "arquivo_origem", "ano_mes_arquivo", "ano_mes_inicio", "in_transbordo", "latitude", "longitude", "sentido_linha")))
cmp = copias.groupBy("codigo_viagem").agg(
    F.count("*").alias("copias"),
    F.countDistinct("arquivo_origem").alias("arquivos"),
    F.countDistinct("cnpj").alias("d_cnpj"), F.countDistinct("placa").alias("d_placa"),
    F.countDistinct("licenca_id").alias("d_licenca"), F.countDistinct("data_inicio").alias("d_inicio"),
    F.countDistinct("data_fim").alias("d_fim"), F.countDistinct("in_transbordo").alias("d_transb"),
    F.countDistinct("latitude").alias("d_lat"), F.countDistinct("sentido_linha").alias("d_sentido"),
    F.min_by("data_fim", "data_inicio").alias("fim_primeira"), F.max("data_inicio").alias("inicio_ultima"),
    F.collect_list("ano_mes_arquivo").alias("meses_arquivo"),
    F.collect_list("ano_mes_inicio").alias("meses_inicio"))
gap = (cmp.withColumn("gap_s", F.col("inicio_ultima").cast("long") - F.col("fim_primeira").cast("long"))
       .select(F.when(F.col("gap_s") < 0, "a) sobreposicao (inicio da 2a antes do fim da 1a)")
               .when(F.col("gap_s") <= 60, "b) continuacao imediata (0 a 60 s)")
               .when(F.col("gap_s") <= 3600, "c) ate 1 hora")
               .when(F.col("gap_s") <= 86400, "d) ate 1 dia")
               .otherwise("e) mais de 1 dia").alias("faixa")))
inv = {
    "faixa_codigo_viagem": [str(x) for x in cmp.agg(F.min(F.col("codigo_viagem").cast("long")), F.max(F.col("codigo_viagem").cast("long"))).collect()[0]],
    "gap_fim_da_primeira_ate_inicio_da_segunda": {x[0]: x[1] for x in gap.groupBy("faixa").count().orderBy("faixa").collect()},
    "codigos_repetidos": cmp.count(),
    "por_numero_copias": {str(x[0]): x[1] for x in cmp.groupBy("copias").count().orderBy("copias").collect()},
    "mesmo_arquivo": cmp.where("arquivos = 1").count(),
    "entre_arquivos": cmp.where("arquivos > 1").count(),
    "copias_com_mesmo_cnpj": cmp.where("d_cnpj = 1").count(),
    "copias_com_mesma_placa": cmp.where("d_placa = 1").count(),
    "copias_com_mesma_licenca": cmp.where("d_licenca = 1").count(),
    "copias_com_mesmo_inicio": cmp.where("d_inicio = 1").count(),
    "copias_com_mesmo_fim": cmp.where("d_fim = 1").count(),
    "copias_totalmente_identicas_nos_campos_de_negocio": cmp.where("d_cnpj=1 and d_placa=1 and d_licenca<=1 and d_inicio=1 and d_fim=1 and d_transb=1 and d_lat=1 and d_sentido=1").count(),
    "mes_inicio_igual_mes_arquivo_em_todas": copias.where("ano_mes_inicio <> ano_mes_arquivo").count() == 0,
    "pares_de_meses_de_arquivo": [[str(x[0]), x[1]] for x in
                                   cmp.where("arquivos > 1").select(F.concat_ws(" + ", F.array_sort(F.array_distinct("meses_arquivo"))).alias("p")).groupBy("p").count().orderBy(F.desc("count")).limit(15).collect()],
}
ex_codes = [r[0] for r in cmp.where("arquivos > 1").select("codigo_viagem").orderBy("codigo_viagem").limit(4).collect()]
inv["exemplos"] = [
    [{k: str(val) for k, val in row.asDict().items()} for row in
     copias.where(F.col("codigo_viagem") == cod).orderBy("arquivo_origem").collect()] for cod in ex_codes]
inv["exemplo_campos_que_diferem"] = [
    r.asDict() for r in cmp.where("arquivos > 1 and (d_cnpj>1 or d_placa>1 or d_licenca>1 or d_inicio>1 or d_fim>1)")
    .select("codigo_viagem", "copias", "d_cnpj", "d_placa", "d_licenca", "d_inicio", "d_fim", "meses_arquivo").limit(5).collect()]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Taxa de ligação viagem -> licença

# COMMAND ----------

L = spark.table(f"{S}.licenca")
orig = L.groupBy("licenca_id").agg(F.collect_set("origem_registro").alias("origens"))
lig = (VF.join(orig, "licenca_id", "left")
       .withColumn("ligada", F.col("origens").isNotNull())
       .withColumn("via_sisaut", F.array_contains("origens", "sisaut"))
       .withColumn("via_continuo", F.array_contains("origens", "continuo")))
tot = lig.agg(F.count("*").alias("n"), F.sum(F.col("ligada").cast("int")).alias("lig"),
              F.sum(F.col("via_sisaut").cast("int")).alias("sis"), F.sum(F.col("via_continuo").cast("int")).alias("cont"),
              F.sum(F.col("licenca_id").isNull().cast("int")).alias("sem_id")).collect()[0]
por_mes = [[x["ano_mes_inicio"], x["n"], x["lig"], round(100.0 * x["lig"] / x["n"], 1)] for x in
           lig.groupBy("ano_mes_inicio").agg(F.count("*").alias("n"), F.sum(F.col("ligada").cast("int")).alias("lig"))
           .orderBy("ano_mes_inicio").collect()]
taxa = round(100.0 * tot["lig"] / tot["n"], 2) if tot["n"] else None
ligacao = {"viagens": tot["n"], "ligadas": tot["lig"], "taxa_pct": taxa, "via_sisaut": tot["sis"], "via_continuo": tot["cont"],
           "licenca_id_nulo": tot["sem_id"], "por_mes": por_mes}

# COMMAND ----------

fl_cont = {r[0]: r[1] for r in VF.select(F.explode("dq_flags").alias("f")).groupBy("f").count().collect()}
r = {
    "bronze_viagens": raw.count(),
    "viagem_fretado": VF.count(),
    "quarentena": Q.count(),
    "quarentena_por_motivo": {x[0]: x[1] for x in Q.groupBy("motivo").count().collect()},
    "duplicatas_codigo_viagem": VF.groupBy("codigo_viagem").count().where("count > 1").count(),
    "reconciliacao_bronze_igual_silver_mais_quarentena": raw.count() == VF.count() + Q.count(),
    "flags": fl_cont,
    "nulos_data_fim": VF.where("data_fim is null").count(),
    "duracao_min_mediana": VF.where("duracao_min is not null").selectExpr("percentile_approx(duracao_min, 0.5)").collect()[0][0],
    "ligacao_viagem_licenca": ligacao,
    "investigacao_codigo_repetido": inv,
}
print(json.dumps(r, ensure_ascii=False, indent=1, default=str))
dbutils.notebook.exit(json.dumps(r, ensure_ascii=False, default=str))
