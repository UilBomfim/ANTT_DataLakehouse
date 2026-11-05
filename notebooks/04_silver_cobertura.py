# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Silver: cobertura por fonte e mês (S1)
# MAGIC
# MAGIC Cria `antt_silver.cobertura_fonte_mes`: para cada fonte e cada mês da janela, se o mês era esperado, se foi publicado
# MAGIC (há arquivo carregado na Bronze), quantas linhas tem e quanto isso representa da mediana dos demais meses publicados.
# MAGIC
# MAGIC Regra: `ausente` = esperado e não publicado; `parcial` = linhas < 25% da mediana dos DEMAIS meses publicados; senão `ok`.
# MAGIC `licencas_continuo` é anual (um arquivo por ano) e fica fora desta tabela: a cobertura anual é simples (2025 e 2026 presentes).
# MAGIC
# MAGIC Escrita: overwrite determinístico da tabela inteira. A tabela é derivada de uma contagem completa da Bronze, então
# MAGIC reexecutar produz o mesmo resultado sem duplicar (MERGE não agrega nada aqui).

# COMMAND ----------

import json
import statistics
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, LongType, StringType, StructField, StructType)

dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.text("data_inicio", "2025-10", "Data início (YYYY-MM)")
dbutils.widgets.text("data_fim", "2026-09", "Data fim (YYYY-MM)")

CATALOG = dbutils.widgets.get("catalog")
DATA_INICIO = dbutils.widgets.get("data_inicio")
DATA_FIM = dbutils.widgets.get("data_fim")
B = f"{CATALOG}.antt_bronze"
S = f"{CATALOG}.antt_silver"
LIMITE_PARCIAL = 0.25

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {S}")

# COMMAND ----------

def meses_janela(ini, fim):
    a, m = map(int, ini.split("-"))
    a2, m2 = map(int, fim.split("-"))
    out = []
    while (a, m) <= (a2, m2):
        out.append(f"{a}-{m:02d}")
        m += 1
        if m == 13:
            a, m = a + 1, 1
    return out


MESES = meses_janela(DATA_INICIO, DATA_FIM)

# (fonte, tabela bronze, filtro opcional, expressão do ano_mes a partir de _source_file)
NUM_MM_AAAA = F.concat(F.regexp_extract("_source_file", r"(\d{2})_(\d{4})\.csv$", 2), F.lit("-"),
                       F.regexp_extract("_source_file", r"(\d{2})_(\d{4})\.csv$", 1))
MAPA_MES = F.create_map(*[x for i, m in enumerate(["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"], 1)
                          for x in (F.lit(m), F.lit(f"{i:02d}"))])
SISAUT_AM = F.concat(F.regexp_extract("_source_file", r"-([a-z]{3})(\d{4})\.csv$", 2), F.lit("-"),
                     MAPA_MES[F.regexp_extract("_source_file", r"-([a-z]{3})(\d{4})\.csv$", 1)])

FONTES = [
    ("viagens_fretado", "bronze_viagens_fretado", None, NUM_MM_AAAA),
    ("paradas_fretado", "bronze_paradas_fretado", None, NUM_MM_AAAA),
    ("bilhetes", "bronze_bilhetes", None, NUM_MM_AAAA),
    ("empresas_fretamento", "bronze_empresas", F.col("tipo_empresa") == "fretamento", NUM_MM_AAAA),
    ("empresas_regular", "bronze_empresas", F.col("tipo_empresa") == "regular", NUM_MM_AAAA),
    ("sisaut", "bronze_sisaut", None, SISAUT_AM),
]

linhas = []
for fonte, tab, filtro, expr in FONTES:
    df = spark.table(f"{B}.{tab}")
    if filtro is not None:
        df = df.where(filtro)
    cont = {r["am"]: (r["n"], r["arqs"]) for r in
            df.groupBy(expr.alias("am")).agg(F.count("*").alias("n"), F.countDistinct("_source_file").alias("arqs")).collect()}
    pub = {m: cont[m][0] for m in MESES if m in cont}
    for m in MESES:
        publicado = m in pub
        n = pub.get(m, 0)
        outros = [v for k, v in pub.items() if k != m]
        med = float(statistics.median(outros)) if outros else None
        pct = round(100.0 * n / med, 1) if (med and publicado) else None
        if not publicado:
            status, obs = "ausente", "mês esperado sem arquivo carregado na Bronze (não publicado na origem)"
        elif med and n < LIMITE_PARCIAL * med:
            status, obs = "parcial", f"{n} linhas, {pct}% da mediana dos demais meses ({med:.0f}); abaixo de 25%"
        else:
            status, obs = "ok", None
        linhas.append((fonte, m, True, publicado, int(n), med, pct, status, obs))

schema = StructType([
    StructField("fonte", StringType()), StructField("ano_mes", StringType()),
    StructField("esperado", BooleanType()), StructField("publicado", BooleanType()),
    StructField("linhas", LongType()), StructField("mediana_outros_meses", DoubleType()),
    StructField("pct_mediana", DoubleType()), StructField("status", StringType()),
    StructField("observacao", StringType()),
])
cob = spark.createDataFrame(linhas, schema)
(cob.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.cobertura_fonte_mes"))
spark.sql(f"COMMENT ON TABLE {S}.cobertura_fonte_mes IS 'Cobertura por fonte e mês: ok, ausente (não publicado) ou parcial (<25% da mediana dos demais meses)'")

# COMMAND ----------

tab = spark.table(f"{S}.cobertura_fonte_mes")
nao_ok = [r.asDict() for r in tab.where("status <> 'ok'").orderBy("fonte", "ano_mes").collect()]
resumo = {
    "linhas_tabela": tab.count(),
    "por_status": {r["status"]: r["n"] for r in tab.groupBy("status").agg(F.count("*").alias("n")).collect()},
    "nao_ok": nao_ok,
    "completa": [[r["fonte"], r["ano_mes"], r["status"], r["linhas"], r["pct_mediana"]]
                 for r in tab.orderBy("fonte", "ano_mes").collect()],
}
print(json.dumps(resumo, ensure_ascii=False, indent=1, default=str))
dbutils.notebook.exit(json.dumps(resumo, ensure_ascii=False, default=str))
