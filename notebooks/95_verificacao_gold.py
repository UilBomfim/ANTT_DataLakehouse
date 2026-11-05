# Databricks notebook source
# MAGIC %md
# MAGIC # 95 - Verificação da Gold (dimensões e fatos)
# MAGIC
# MAGIC Somente leitura. Devolve resumo pequeno. A parte 2 (indicadores `agg_*`) reaproveita este notebook: acrescente os
# MAGIC `agg_*` em `CONTAGENS` e as chaves em `CHAVES`.
# MAGIC
# MAGIC Verificações: (a) contagens > 0; (c) fato_viagem = silver.viagem_fretado e as cópias descartadas dos 57 códigos repetidos
# MAGIC fora da fato; (d) chaves únicas; (e) soma de bilhetes por status = total da Silver; (f) % de resolução de origem/destino/cnpj;
# MAGIC (g) p95 e distribuição por status (recalculados a partir de `fato_bilhete`).
# MAGIC A verificação (b) (reexecução não muda contagens) compara as contagens devolvidas por duas execuções dos notebooks.

# COMMAND ----------

import json
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace", "Catálogo")
CATALOG = dbutils.widgets.get("catalog")
S = f"{CATALOG}.antt_silver"
G = f"{CATALOG}.antt_gold"
T = lambda n: spark.table(f"{G}.{n}")

CONTAGENS = ["dim_empresa", "dim_localidade", "dim_data", "dim_cobertura", "cobertura_fonte_mes",
             "fato_viagem", "fato_licenca", "fato_bilhete"]
CHAVES = {"dim_empresa": ["cnpj"], "fato_viagem": ["codigo_viagem"], "fato_licenca": ["licenca_id", "origem_registro"],
          "dim_data": ["data"], "dim_localidade": ["localidade_key"], "dim_cobertura": ["fonte", "ano_mes"],
          "cobertura_fonte_mes": ["fonte", "ano_mes"]}

r = {}
cont = {n: T(n).count() for n in CONTAGENS}
r["a_contagens"] = cont
r["a_ok"] = all(v > 0 for v in cont.values())

dups = {n: T(n).groupBy(*k).count().where("count > 1").count() for n, k in CHAVES.items()}
r["d_chaves_duplicadas"] = dups
r["d_ok"] = all(v == 0 for v in dups.values())

# (c)
sv = spark.table(f"{S}.viagem_fretado").count()
q = spark.table(f"{S}.quarentena_viagem_fretado").where("motivo like 'codigo_repetido%'")
fv = T("fato_viagem")
descartadas_na_fato = q.join(fv, (q.codigo_viagem == fv.codigo_viagem) & (q.data_inicio == fv.data_inicio) & (q.data_fim == fv.data_fim)).count()
codigos_na_fato = q.select("codigo_viagem").distinct().join(fv.select("codigo_viagem"), "codigo_viagem").count()
r["c_fato_viagem"] = cont["fato_viagem"]
r["c_silver_viagem"] = sv
r["c_copias_descartadas_na_fato"] = descartadas_na_fato
r["c_codigos_repetidos_quarentena"] = q.select("codigo_viagem").distinct().count()
r["c_codigos_repetidos_com_copia_mantida_na_fato"] = codigos_na_fato
r["c_ok"] = cont["fato_viagem"] == sv and descartadas_na_fato == 0

# (e)
fb = T("fato_bilhete")
tot_silver = spark.table(f"{S}.bilhete_agregado").agg(F.sum("quantidade_bilhetes")).collect()[0][0]
st = fb.groupBy("status_mes_viagem").agg(F.count("*").alias("linhas"), F.sum("quantidade_bilhetes").alias("bilhetes"),
                                         F.min("ano_mes_viagem").alias("de"), F.max("ano_mes_viagem").alias("ate")).collect()
r["e_total_silver"] = tot_silver
r["e_total_por_status"] = sum(x["bilhetes"] for x in st)
r["e_ok"] = r["e_total_silver"] == r["e_total_por_status"] and fb.where("status_mes_viagem is null").count() == 0
r["g_distribuicao_status"] = {x["status_mes_viagem"]: {"linhas": x["linhas"], "bilhetes": x["bilhetes"],
                                                        "pct_bilhetes": round(100.0 * x["bilhetes"] / tot_silver, 2),
                                                        "de": x["de"], "ate": x["ate"]} for x in st}

# (g) p95 recalculado da fato
ant = F.months_between("mes_viagem", "mes_emissao").cast("int")
hist = {int(x[0]): int(x[1]) for x in fb.groupBy(ant.alias("a")).agg(F.sum("quantidade_bilhetes")).where("a is not null").collect()}
tot = sum(hist.values())


def pct(p):
    acc = 0
    for a in sorted(hist):
        acc += hist[a]
        if acc >= p * tot:
            return a


r["g_antecedencia_meses"] = {"media": round(sum(a * q_ for a, q_ in hist.items()) / tot, 3), "p50": pct(.5), "p90": pct(.9),
                             "p95": pct(.95), "p99": pct(.99), "negativa_bilhetes": sum(q_ for a, q_ in hist.items() if a < 0)}

# (f)
n = cont["fato_viagem"]
f = fv.agg(
    F.sum(F.col("localidade_origem_key").isNotNull().cast("int")).alias("o_key"),
    F.sum(F.col("localidade_destino_key").isNotNull().cast("int")).alias("d_key"),
    F.sum(F.col("licenca_id").isNull().cast("int")).alias("sem_lic_id")).collect()[0]
loc = T("dim_localidade").select("localidade_key")
o_res = fv.join(F.broadcast(loc), fv.localidade_origem_key == loc.localidade_key).count()
d_res = fv.join(F.broadcast(loc), fv.localidade_destino_key == loc.localidade_key).count()
emp_res = fv.join(T("dim_empresa").select("cnpj"), "cnpj").count()
p = lambda x: round(100.0 * x / n, 2)
r["f_viagem_pct"] = {"origem_resolvida": p(o_res), "destino_resolvido": p(d_res), "cnpj_em_dim_empresa": p(emp_res),
                     "origem_key_nao_nula": p(f["o_key"]), "destino_key_nao_nulo": p(f["d_key"])}
fl = T("fato_licenca")
nl = cont["fato_licenca"]
r["f_licenca_pct"] = {
    "origem_resolvida": round(100.0 * fl.join(F.broadcast(loc), fl.localidade_origem_key == loc.localidade_key).count() / nl, 2),
    "destino_resolvido": round(100.0 * fl.join(F.broadcast(loc), fl.localidade_destino_key == loc.localidade_key).count() / nl, 2),
    "cnpj_em_dim_empresa": round(100.0 * fl.join(T("dim_empresa").select("cnpj"), "cnpj").count() / nl, 2)}
nb = cont["fato_bilhete"]
r["f_bilhete_pct"] = {
    "origem_resolvida": round(100.0 * fb.join(F.broadcast(loc), fb.localidade_origem_key == loc.localidade_key).count() / nb, 2),
    "destino_resolvido": round(100.0 * fb.join(F.broadcast(loc), fb.localidade_destino_key == loc.localidade_key).count() / nb, 2)}
r["extra_paradas_confiaveis_pct"] = round(100.0 * fv.where("paradas_confiaveis").count() / n, 2)
r["extra_viagens_com_parada"] = fv.where("qtd_paradas > 0").count()
r["extra_soma_paradas_fato"] = fv.agg(F.sum("qtd_paradas")).collect()[0][0]
r["extra_paradas_mes_parcial_false_silver"] = spark.table(f"{S}.parada_fretado").where("not coalesce(mes_parcial, false)").count()

r["ok"] = r["a_ok"] and r["c_ok"] and r["d_ok"] and r["e_ok"]
print(json.dumps(r, ensure_ascii=False, indent=1, default=str))
dbutils.notebook.exit(json.dumps(r, ensure_ascii=False, default=str))
