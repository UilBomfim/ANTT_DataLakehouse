# Databricks notebook source
# MAGIC %md
# MAGIC # 99 - Verificação da Bronze
# MAGIC Contagens por tabela, checagem de acentuação e amostras. Somente leitura.

# COMMAND ----------

import json
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace", "Catálogo")
CATALOG = dbutils.widgets.get("catalog")
B = f"{CATALOG}.antt_bronze"
TABELAS = ["viagens_fretado", "paradas_fretado", "bilhetes", "empresas", "licencas_continuo", "sisaut"]

# Padrões de texto corrompido: caractere de substituição, ou mojibake típico de utf-8 lido como latin1
PADRAO = "\ufffd|Ã[\u0080-\u00bf]|Â[\u0080-\u00bf]"

out = {"linhas": {}, "arquivos": {}, "corrompidos": {}, "colunas": {}}
for t in TABELAS:
    df = spark.table(f"{B}.bronze_{t}")
    out["linhas"][t] = df.count()
    out["arquivos"][t] = df.select("_source_file").distinct().count()
    negocio = [c for c in df.columns if not c.startswith("_")]
    out["colunas"][t] = len(negocio)
    texto = F.concat_ws("|", *[F.col(f"`{c}`") for c in negocio])
    out["corrompidos"][t] = df.where(texto.rlike(PADRAO)).count()

b = spark.table(f"{B}.bronze_bilhetes")
out["bilhetes_alagoinhas"] = b.where(F.col("ponto_origem_viagem") == "Alagoinhas/BA").count()
out["bilhetes_macae"] = b.where(F.col("ponto_origem_viagem") == "Macaé/RJ").count()
out["bilhetes_exemplos_acento"] = [r[0] for r in b.where(F.col("ponto_origem_viagem").rlike("[À-ÿ]"))
                                   .select("ponto_origem_viagem").distinct().limit(8).collect()]
out["viagens_in_transbordo"] = {r[0]: r[1] for r in spark.table(f"{B}.bronze_viagens_fretado")
                                .groupBy("in_transbordo").count().collect()}
out["sisaut_exemplos_acento"] = [r[0] for r in spark.table(f"{B}.bronze_sisaut")
                                 .where(F.col("municipio_origem").rlike("[À-ÿ]"))
                                 .select("municipio_origem").distinct().limit(5).collect()]
out["paradas_exemplos_acento"] = [r[0] for r in spark.table(f"{B}.bronze_paradas_fretado")
                                  .where(F.col("motivo_parada").rlike("[À-ÿ]"))
                                  .select("motivo_parada").distinct().limit(5).collect()]
out["empresas_por_tipo"] = {r[0]: r[1] for r in spark.table(f"{B}.bronze_empresas").groupBy("tipo_empresa").count().collect()}
out["manifesto"] = {f"{r.fonte}/{r.status}": r.n for r in
                    spark.table(f"{CATALOG}.antt_raw.ingest_manifest").groupBy("fonte", "status")
                    .agg(F.count("*").alias("n")).collect()}

# COMMAND ----------

print(json.dumps(out, ensure_ascii=False, indent=2))
dbutils.notebook.exit(json.dumps(out, ensure_ascii=False, default=str))
