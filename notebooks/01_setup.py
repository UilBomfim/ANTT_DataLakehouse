# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Setup do Lakehouse ANTT
# MAGIC
# MAGIC Cria (de forma idempotente) os schemas `antt_raw`, `antt_bronze`, `antt_silver`, `antt_gold`,
# MAGIC o Volume `antt_raw.landing` e a tabela de controle `antt_raw.ingest_manifest`.

# COMMAND ----------

import json

dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.text("volume", "landing", "Volume")

CATALOG = dbutils.widgets.get("catalog")
VOLUME = dbutils.widgets.get("volume")

SCHEMAS = ["antt_raw", "antt_bronze", "antt_silver", "antt_gold"]

# COMMAND ----------

for s in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.antt_raw.{VOLUME}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.antt_raw.ingest_manifest (
  fonte         STRING    COMMENT 'Chave da fonte (ex.: viagens_fretado)',
  recurso       STRING    COMMENT 'Nome do recurso no CKAN',
  url           STRING    COMMENT 'URL de download',
  last_modified STRING    COMMENT 'last_modified informado pelo CKAN',
  bytes         BIGINT    COMMENT 'Bytes gravados no Volume',
  sha256        STRING    COMMENT 'SHA-256 do arquivo baixado',
  baixado_em    TIMESTAMP COMMENT 'Data/hora do download',
  status        STRING    COMMENT 'ok | erro',
  mensagem      STRING    COMMENT 'Detalhe do erro ou observação'
)
COMMENT 'Histórico de downloads da camada landing (append only)'
""")

# COMMAND ----------

existentes = [r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()]
volumes = [r.volume_name for r in spark.sql(f"SHOW VOLUMES IN {CATALOG}.antt_raw").collect()]
manifesto_ok = spark.catalog.tableExists(f"{CATALOG}.antt_raw.ingest_manifest")

resumo = {
    "catalog": CATALOG,
    "schemas": {s: s in existentes for s in SCHEMAS},
    "volume": f"/Volumes/{CATALOG}/antt_raw/{VOLUME}",
    "volume_existe": VOLUME in volumes,
    "ingest_manifest": manifesto_ok,
}
print(resumo)
dbutils.notebook.exit(json.dumps(resumo, ensure_ascii=False))
