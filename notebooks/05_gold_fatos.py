# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Gold: fatos (G7, G4 parte 1)
# MAGIC
# MAGIC Schema `workspace.antt_gold`. Escrita: **overwrite determinístico**. Os fatos são derivados por completo da Silver e de
# MAGIC `dim_cobertura`; reexecutar dá o mesmo resultado e não duplica. Rodar antes `05_gold_dimensoes`.
# MAGIC
# MAGIC | Tabela | Grão / chave |
# MAGIC |---|---|
# MAGIC | `fato_viagem` | 1 viagem de `silver.viagem_fretado`; chave `codigo_viagem`. Os 57 códigos repetidos entre arquivos: as cópias descartadas ficam em `silver.quarentena_viagem_fretado` e não entram; a cópia mantida segue na Silver (76.802) com a flag `codigo_repetido_entre_arquivos` |
# MAGIC | `fato_licenca` | 1 licença; chave `licenca_id + origem_registro` (`sisaut`/`continuo`) |
# MAGIC | `fato_bilhete` | grão de `silver.bilhete_agregado`: mes_viagem × mes_emissao × origem × destino × tipo_servico_norm × tipo_gratuidade_norm |
# MAGIC
# MAGIC ## Regra `status_mes_viagem` (fato_bilhete, decisão E2/G4)
# MAGIC
# MAGIC Os arquivos de bilhetes são por **mês de emissão** e a janela vai de `data_inicio` ao último mês de emissão carregado.
# MAGIC Por mês da viagem, os primeiros meses perdem bilhetes vendidos antes da janela, e meses de viagem depois do último mês de
# MAGIC emissão ainda não aconteceram. Antecedência = `mes_viagem - mes_emissao` em meses, ponderada por `quantidade_bilhetes`.
# MAGIC `p95` = menor antecedência cuja frequência acumulada ponderada chega a 95%.
# MAGIC
# MAGIC - `incompleto_inicio`: `mes_viagem < data_inicio + p95 meses`
# MAGIC - `confiavel`: do limite acima até o último mês de emissão carregado
# MAGIC - `agendado`: `mes_viagem > último mês de emissão carregado` (fora do volume transportado)
# MAGIC
# MAGIC O p95 e a distribuição da antecedência são devolvidos no `dbutils.notebook.exit` e gravados no comentário de `fato_bilhete`.

# COMMAND ----------

import json
from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.text("data_inicio", "2025-10", "Data início (YYYY-MM)")
dbutils.widgets.text("data_fim", "2026-09", "Data fim (YYYY-MM)")
CATALOG = dbutils.widgets.get("catalog")
DATA_INICIO = dbutils.widgets.get("data_inicio")
S = f"{CATALOG}.antt_silver"
G = f"{CATALOG}.antt_gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {G}")

ACC_FROM = "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ"
ACC_TO = "AAAAAEEEEIIIIOOOOOUUUUCN"


# mesmas funções de 05_gold_dimensoes (a chave tem de ser idêntica)
def norm_txt(c):
    t = F.regexp_replace(F.trim(F.translate(F.upper(c), ACC_FROM, ACC_TO)), r"\s+", " ")
    return F.nullif(t, F.lit(""))


def loc_key(mun, uf):
    mun, uf = norm_txt(mun), F.nullif(F.upper(F.trim(uf)), F.lit(""))
    return F.when(mun.isNotNull() & uf.isNotNull(), F.concat_ws("|", mun, uf))


def save(df, name, comment):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{G}.{name}")
    spark.sql(f"COMMENT ON TABLE {G}.{name} IS '{comment}'")


# COMMAND ----------
# fato_licenca

lic = spark.table(f"{S}.licenca")
fato_licenca = lic.select(
    "licenca_id", "origem_registro", "cnpj",
    loc_key(F.col("municipio_origem"), F.col("uf_origem")).alias("localidade_origem_key"),
    loc_key(F.col("municipio_destino"), F.col("uf_destino")).alias("localidade_destino_key"),
    "data_inicio", F.date_format("data_inicio", "yyyy-MM").alias("ano_mes_inicio"),
    F.lit(1).alias("viagens_licenciadas"), "total_passageiros", "distancia_total")
save(fato_licenca, "fato_licenca",
     "Grao: 1 licenca. Chave: licenca_id+origem_registro (sisaut/continuo). viagens_licenciadas=1. Origem: silver.licenca")

# COMMAND ----------
# fato_viagem

v = spark.table(f"{S}.viagem_fretado")
l1 = (lic.select("licenca_id", "origem_registro", "total_passageiros",
                 loc_key(F.col("municipio_origem"), F.col("uf_origem")).alias("localidade_origem_key"),
                 loc_key(F.col("municipio_destino"), F.col("uf_destino")).alias("localidade_destino_key"))
      .withColumn("_rn", F.row_number().over(Window.partitionBy("licenca_id").orderBy("origem_registro")))
      .where("_rn = 1").drop("_rn", "origem_registro"))  # licenca_id é único na Silver (0 em ambas as origens); a dedup só protege o grão

# paradas confiáveis: só paradas de arquivo sem mes_parcial; viagem sem parada = 0
par = (spark.table(f"{S}.parada_fretado").where(~F.coalesce("mes_parcial", F.lit(False)))
       .groupBy("codigo_viagem").agg(F.count("*").alias("qtd_paradas")))
cob_par = (spark.table(f"{G}.dim_cobertura").where("fonte = 'paradas_fretado'")
           .select(F.col("ano_mes").alias("ano_mes_inicio"), (F.col("status") == "ok").alias("_par_ok")))

fato_viagem = (v.join(l1, "licenca_id", "left").join(par, "codigo_viagem", "left").join(F.broadcast(cob_par), "ano_mes_inicio", "left")
               .select("codigo_viagem", "cnpj", "licenca_id", "localidade_origem_key", "localidade_destino_key",
                       "data_inicio", "data_fim", "ano_mes_inicio", "duracao_min",
                       F.coalesce("qtd_paradas", F.lit(0)).cast("int").alias("qtd_paradas"),
                       F.coalesce("_par_ok", F.lit(False)).alias("paradas_confiaveis"),
                       F.col("total_passageiros").alias("passageiros_licenca"), "dq_flags", "in_transbordo"))
save(fato_viagem, "fato_viagem",
     "Grao: 1 viagem (silver.viagem_fretado). Chave: codigo_viagem. Copias descartadas dos 57 codigos repetidos ficam na quarentena. qtd_paradas so com mes_parcial=false; paradas_confiaveis=false se o mes tem status parcial/ausente para paradas")

# COMMAND ----------
# fato_bilhete: antecedência ponderada e status_mes_viagem

b = spark.table(f"{S}.bilhete_agregado")
ant = F.months_between(F.col("mes_viagem"), F.col("mes_emissao")).cast("int")
hist = {int(x[0]): int(x[1]) for x in b.groupBy(ant.alias("a")).agg(F.sum("quantidade_bilhetes").alias("q")).where("a is not null").collect()}
total = sum(hist.values())


def pond_pct(p):
    acc = 0
    for a in sorted(hist):
        acc += hist[a]
        if acc >= p * total:
            return a


media = sum(a * q for a, q in hist.items()) / total
P = {"media": round(media, 3), "p50": pond_pct(0.50), "p90": pond_pct(0.90), "p95": pond_pct(0.95), "p99": pond_pct(0.99)}
P95 = P["p95"]
ultimo_emissao = b.agg(F.max("mes_emissao")).collect()[0][0]
limite = spark.sql(f"select add_months(to_date('{DATA_INICIO}-01'), {P95})").collect()[0][0]

status = (F.when(F.col("mes_viagem") < F.lit(limite), "incompleto_inicio")
           .when(F.col("mes_viagem") > F.lit(ultimo_emissao), "agendado")
           .otherwise("confiavel"))
fato_bilhete = b.select(
    "mes_viagem", "mes_emissao",
    F.date_format("mes_viagem", "yyyy-MM").alias("ano_mes_viagem"), F.date_format("mes_emissao", "yyyy-MM").alias("ano_mes_emissao"),
    loc_key(F.col("cidade_origem"), F.col("uf_origem")).alias("localidade_origem_key"),
    loc_key(F.col("cidade_destino"), F.col("uf_destino")).alias("localidade_destino_key"),
    "tipo_servico_norm", "tipo_gratuidade_norm", "quantidade_bilhetes", "media_valor_total",
    status.alias("status_mes_viagem"))
save(fato_bilhete, "fato_bilhete",
     f"Grao: silver.bilhete_agregado (mes_viagem x mes_emissao x origem x destino x servico x gratuidade). status_mes_viagem: incompleto_inicio se mes_viagem < {DATA_INICIO}+{P95} meses (p95 da antecedencia ponderada); confiavel ate o ultimo mes de emissao ({ultimo_emissao}); agendado depois")

# COMMAND ----------

nomes = ["fato_viagem", "fato_licenca", "fato_bilhete"]
fb = spark.table(f"{G}.fato_bilhete")
dist = {x[0]: {"linhas": x[1], "bilhetes": x[2], "de": str(x[3]), "ate": str(x[4])} for x in
        fb.groupBy("status_mes_viagem").agg(F.count("*"), F.sum("quantidade_bilhetes"), F.min("mes_viagem"), F.max("mes_viagem")).collect()}
r = {
    "contagens": {n: spark.table(f"{G}.{n}").count() for n in nomes},
    "antecedencia_meses": P, "limite_incompleto_inicio_antes_de": str(limite), "ultimo_mes_emissao": str(ultimo_emissao),
    "status_mes_viagem": dist,
}
print(json.dumps(r, ensure_ascii=False, indent=1))
dbutils.notebook.exit(json.dumps(r, ensure_ascii=False))
