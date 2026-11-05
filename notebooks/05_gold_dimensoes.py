# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Gold: dimensões (G7)
# MAGIC
# MAGIC Schema `workspace.antt_gold`. Todas as tabelas são derivadas por completo da Silver (ou geradas), então a escrita é
# MAGIC **overwrite determinístico**: reexecutar dá o mesmo resultado e não duplica. Um MERGE só manteria linhas de entidades
# MAGIC que saíram da Silver.
# MAGIC
# MAGIC | Tabela | Grão / chave | Origem |
# MAGIC |---|---|---|
# MAGIC | `dim_empresa` | 1 empresa; chave `cnpj` (14 dígitos). `tipo` = fretamento / regular / ambos | `silver.empresa` |
# MAGIC | `dim_localidade` | 1 município+UF; chave `localidade_key` = `MUNICIPIO\|UF` (maiúsculas, sem acento, espaços colapsados) | origens e destinos de `silver.licenca`, `silver.licenca_trecho`, `silver.bilhete_agregado` |
# MAGIC | `dim_data` | 1 dia; chave `data` (2025-01-01 a 2027-12-31) | gerada |
# MAGIC | `dim_cobertura` | 1 fonte × mês; chave `fonte + ano_mes`; status `ok`/`ausente`/`parcial` | `silver.cobertura_fonte_mes` |
# MAGIC | `cobertura_fonte_mes` | tabela de consumo, cópia de `dim_cobertura` (G6) | `dim_cobertura` |
# MAGIC
# MAGIC `localidade_key` é a mesma função usada em `05_gold_fatos` (mantê-las iguais). Linhas de origem/destino sem município ou sem UF
# MAGIC não geram chave (ficam nulas nos fatos).

# COMMAND ----------

import json
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.text("data_inicio", "2025-10", "Data início (YYYY-MM)")
dbutils.widgets.text("data_fim", "2026-09", "Data fim (YYYY-MM)")
CATALOG = dbutils.widgets.get("catalog")
S = f"{CATALOG}.antt_silver"
G = f"{CATALOG}.antt_gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {G}")

ACC_FROM = "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ"
ACC_TO = "AAAAAEEEEIIIIOOOOOUUUUCN"
UFS = "AC AL AP AM BA CE DF ES GO MA MT MS MG PA PB PR PE PI RJ RN RS RO RR SC SP SE TO".split()


def norm_txt(c):
    t = F.regexp_replace(F.trim(F.translate(F.upper(c), ACC_FROM, ACC_TO)), r"\s+", " ")
    return F.nullif(t, F.lit(""))


def loc_key(mun, uf):
    return F.when(mun.isNotNull() & uf.isNotNull(), F.concat_ws("|", mun, uf))


def save(df, name, comment):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{G}.{name}")
    spark.sql(f"COMMENT ON TABLE {G}.{name} IS '{comment}'")


# COMMAND ----------
# dim_empresa

e = spark.table(f"{S}.empresa")
dim_empresa = e.select(
    "cnpj", "razao_social",
    F.when(F.coalesce("tem_fretamento", F.lit(False)) & F.coalesce("tem_regular", F.lit(False)), F.lit("ambos"))
     .when(F.coalesce("tem_fretamento", F.lit(False)), F.lit("fretamento"))
     .when(F.coalesce("tem_regular", F.lit(False)), F.lit("regular"))
     .otherwise(F.col("tipo_empresa")).alias("tipo"),
    "numero_taf", "numero_tar", "vigencia", "snapshot_mes")
save(dim_empresa, "dim_empresa",
     "Grao: 1 empresa. Chave: cnpj (14 digitos). tipo = fretamento/regular/ambos. Origem: silver.empresa")

# COMMAND ----------
# dim_localidade

def pares(df, mun_o, uf_o, mun_d, uf_d, fonte):
    a = df.select(norm_txt(F.col(mun_o)).alias("municipio"), F.upper(F.trim(F.col(uf_o))).alias("uf"))
    b = df.select(norm_txt(F.col(mun_d)).alias("municipio"), F.upper(F.trim(F.col(uf_d))).alias("uf"))
    return a.unionByName(b).withColumn("fonte", F.lit(fonte))


loc = (pares(spark.table(f"{S}.licenca"), "municipio_origem", "uf_origem", "municipio_destino", "uf_destino", "licenca")
       .unionByName(pares(spark.table(f"{S}.licenca_trecho"), "municipio_inicio_trecho", "uf_inicio_trecho",
                          "municipio_final_trecho", "uf_final_trecho", "licenca_trecho"))
       .unionByName(pares(spark.table(f"{S}.bilhete_agregado"), "cidade_origem", "uf_origem",
                          "cidade_destino", "uf_destino", "bilhete")))
loc = loc.withColumn("uf", F.nullif("uf", F.lit(""))).withColumn("localidade_key", loc_key(F.col("municipio"), F.col("uf")))
dim_localidade = (loc.where("localidade_key is not null")
                  .groupBy("localidade_key")
                  .agg(F.first("municipio").alias("municipio"), F.first("uf").alias("uf"),
                       F.array_sort(F.collect_set("fonte")).alias("fontes"))
                  .withColumn("uf_valida", F.col("uf").isin(UFS))
                  .select("localidade_key", "municipio", "uf", "uf_valida", "fontes"))
save(dim_localidade, "dim_localidade",
     "Grao: 1 municipio+UF. Chave: localidade_key = MUNICIPIO|UF normalizado. Origem: licenca, licenca_trecho, bilhete_agregado")

# COMMAND ----------
# dim_data

MESES = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]
DIAS = ["segunda", "terça", "quarta", "quinta", "sexta", "sábado", "domingo"]
d = spark.sql("select explode(sequence(date'2025-01-01', date'2027-12-31', interval 1 day)) as data")
dim_data = d.select(
    "data",
    F.year("data").alias("ano"),
    F.quarter("data").alias("trimestre"),
    F.month("data").alias("mes"),
    F.date_format("data", "yyyy-MM").alias("ano_mes"),
    (F.expr("weekday(data)") + 1).alias("dia_semana_num"),
    F.element_at(F.array(*[F.lit(x) for x in DIAS]), F.expr("weekday(data)") + 1).alias("dia_semana"),
    F.element_at(F.array(*[F.lit(x) for x in MESES]), F.month("data")).alias("mes_nome"))
save(dim_data, "dim_data", "Grao: 1 dia (2025-01-01 a 2027-12-31). Chave: data. dia_semana_num 1=segunda")

# COMMAND ----------
# dim_cobertura e tabela de consumo cobertura_fonte_mes

c = spark.table(f"{S}.cobertura_fonte_mes")
dim_cobertura = c.select("fonte", "ano_mes", "status", "linhas", "pct_mediana", "observacao",
                         "esperado", "publicado", (F.col("status") == "ok").alias("mes_completo"))
save(dim_cobertura, "dim_cobertura",
     "Grao: 1 fonte x mes. Chave: fonte+ano_mes. status ok/ausente/parcial; mes_completo = status ok. Origem: silver.cobertura_fonte_mes")
save(spark.table(f"{G}.dim_cobertura"), "cobertura_fonte_mes",
     "Tabela de consumo (G6). Copia de dim_cobertura. Grao: fonte x mes. Chave: fonte+ano_mes")

# COMMAND ----------

nomes = ["dim_empresa", "dim_localidade", "dim_data", "dim_cobertura", "cobertura_fonte_mes"]
r = {"contagens": {n: spark.table(f"{G}.{n}").count() for n in nomes}}
r["dim_empresa_por_tipo"] = {x[0]: x[1] for x in spark.table(f"{G}.dim_empresa").groupBy("tipo").count().collect()}
r["dim_localidade_uf_invalida"] = spark.table(f"{G}.dim_localidade").where("not uf_valida").count()
r["dim_cobertura_por_status"] = {x[0]: x[1] for x in spark.table(f"{G}.dim_cobertura").groupBy("status").count().collect()}
print(json.dumps(r, ensure_ascii=False, indent=1))
dbutils.notebook.exit(json.dumps(r, ensure_ascii=False))
