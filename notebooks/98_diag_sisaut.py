# Databricks notebook source
# MAGIC %md
# MAGIC # 98 - Diagnóstico de texto corrompido no SISAUT (somente leitura)

# COMMAND ----------

import json
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace", "Catálogo")
CATALOG = dbutils.widgets.get("catalog")
df = spark.table(f"{CATALOG}.antt_bronze.bronze_sisaut")
PADRAO = "\ufffd|Ã[\u0080-\u00bf]|Â[\u0080-\u00bf]"
negocio = [c for c in df.columns if not c.startswith("_")]

out = {"por_coluna": {}, "por_arquivo": {}, "exemplos": []}
for c in negocio:
    n = df.where(F.col(f"`{c}`").rlike(PADRAO)).count()
    if n:
        out["por_coluna"][c] = n

texto = F.concat_ws("|", *[F.col(f"`{c}`") for c in negocio])
ruim = df.where(texto.rlike(PADRAO))
out["por_arquivo"] = {r[0].split("/")[-1]: r[1] for r in ruim.groupBy("_source_file").count().collect()}

col = next(iter(out["por_coluna"]), None)
if col:
    ex = ruim.where(F.col(f"`{col}`").rlike(PADRAO)).select(col).distinct().limit(12).collect()
    out["exemplos"] = [{"coluna": col, "valor": r[0], "codepoints": [hex(ord(ch)) for ch in r[0] if ord(ch) > 127][:8]} for r in ex]
    out["distintos_ruins_na_coluna"] = ruim.where(F.col(f"`{col}`").rlike(PADRAO)).select(col).distinct().count()

print(json.dumps(out, ensure_ascii=False, indent=2))
dbutils.notebook.exit(json.dumps(out, ensure_ascii=False))
