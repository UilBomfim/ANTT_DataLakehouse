# Databricks notebook source
# MAGIC %md
# MAGIC # 95 - Verificação dos indicadores da Gold (`agg_*`)
# MAGIC Somente leitura; devolve resumo pequeno. Complementa `95_verificacao_gold` (dimensões e fatos).
# MAGIC Verifica: (a) contagens > 0; (e) fev/2026 nulo e status `ausente` para viagens realizadas, e `viagens_licenciadas` NÃO nulo;
# MAGIC paradas de ago/2026 parcial; (f) bilhetes coerentes; (g) ranking sem empates e passageiros sem dupla contagem.

# COMMAND ----------

import json
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace", "Catálogo")
CATALOG = dbutils.widgets.get("catalog")
S, G = f"{CATALOG}.antt_silver", f"{CATALOG}.antt_gold"
T = lambda n: spark.table(f"{G}.{n}")
r = {}

AGG = ["agg_viagens_periodo", "agg_ranking_empresas", "agg_origem_destino", "agg_duracao_media", "agg_qualidade_registros",
       "agg_evolucao_mensal_volume"]
r["a_contagens"] = {n: T(n).count() for n in AGG}
r["a_ok"] = all(v > 0 for v in r["a_contagens"].values())

# (e) fevereiro/2026
fev_p = T("agg_viagens_periodo").where("nivel = 'mes' and periodo = '2026-02'").collect()[0]
fev_e = T("agg_evolucao_mensal_volume").where("ano_mes = '2026-02'").collect()[0]
fev_d = T("agg_duracao_media").where("nivel = 'mes' and periodo = '2026-02'").collect()[0]
r["e_fev2026"] = {
    "periodo": {"viagens_realizadas": fev_p["viagens_realizadas"], "viagens_licenciadas": fev_p["viagens_licenciadas"], "status": fev_p["status_cobertura"]},
    "evolucao": {"viagens_realizadas": fev_e["viagens_realizadas"], "viagens_licenciadas": fev_e["viagens_licenciadas"],
                 "status": fev_e["status_cobertura_viagens"], "mom": fev_e["viagens_realizadas_mom_pct"]},
    "duracao": {"media": fev_d["duracao_media_min"], "status": fev_d["status_cobertura"]}}
r["e_ok"] = (fev_p["viagens_realizadas"] is None and fev_p["status_cobertura"] == "ausente" and fev_p["viagens_licenciadas"] is not None
             and fev_e["viagens_realizadas"] is None and fev_e["viagens_licenciadas"] is not None and fev_d["duracao_media_min"] is None)
mar = T("agg_evolucao_mensal_volume").where("ano_mes = '2026-03'").collect()[0]
r["e_mar2026_mom_nulo_porque_fev_ausente"] = mar["viagens_realizadas_mom_pct"] is None
par_ago = T("dim_cobertura").where("fonte = 'paradas_fretado' and ano_mes = '2026-08'").collect()[0]["status"]
r["e_paradas_ago2026_status"] = par_ago

# (f) bilhetes: transportados confiáveis + vendidos
ev = T("agg_evolucao_mensal_volume").orderBy("ano_mes").collect()
r["f_bilhetes_por_mes"] = [[x["ano_mes"], x["bilhetes_transportados"], x["status_mes_viagem"], x["bilhetes_vendidos"]] for x in ev]
tot_silver = spark.table(f"{S}.bilhete_agregado").agg(F.sum("quantidade_bilhetes")).collect()[0][0]
r["f_total_silver"] = tot_silver
r["f_total_vendidos_janela_gold"] = T("fato_bilhete").agg(F.sum("quantidade_bilhetes")).collect()[0][0]

# (g) ranking: sem empates de rank e sem dupla contagem de passageiros
rk = T("agg_ranking_empresas")
emp_rank_dup = rk.groupBy("periodo_tipo", "periodo", "rank_viagens").count().where("count > 1").count()
r["g_ranking_ranks_duplicados"] = emp_rank_dup
top = rk.where("periodo_tipo = 'total'").orderBy("rank_viagens").limit(1).collect()[0]
sv = spark.table(f"{S}.viagem_fretado").where(F.col("cnpj") == top["cnpj"])
sl = (spark.table(f"{S}.licenca").join(sv.select("licenca_id").distinct(), "licenca_id")
      .groupBy("licenca_id").agg(F.max("total_passageiros").alias("p")).agg(F.sum("p")).collect()[0][0])
r["g_exemplo_top_empresa"] = {"cnpj": top["cnpj"], "razao": top["razao_social"], "viagens_agg": top["viagens"],
                              "viagens_silver": sv.count(), "passageiros_agg": top["passageiros"], "passageiros_silver_1x_por_licenca": sl}
r["g_ok"] = emp_rank_dup == 0 and top["viagens"] == sv.count() and top["passageiros"] == sl

# top 10 empresas, top 10 origens/destinos, duração por mês, qualidade resumida (para o relatório)
r["resp_top10_empresas"] = [[x["rank_viagens"], x["razao_social"], x["viagens"], x["passageiros"]]
                            for x in rk.where("periodo_tipo = 'total'").orderBy("rank_viagens").limit(10).collect()]
od = T("agg_origem_destino")
r["resp_top10_od"] = {f"{v}_{t}": [[x["localidade_key"], x["viagens"], x["passageiros"]] for x in
                                    od.where(f"visao = '{v}' and tipo = '{t}'").orderBy("rank").limit(10).collect()]
                      for v in ["fretado", "regular"] for t in ["origem", "destino", "par"]}
r["resp_duracao_mes"] = [[x["periodo"], x["viagens_consideradas"], x["duracao_media_min"], x["duracao_mediana_min"], x["status_cobertura"]]
                         for x in T("agg_duracao_media").where("nivel = 'mes'").orderBy("periodo").collect()]
r["resp_viagens_mes"] = [[x["periodo"], x["viagens_realizadas"], x["viagens_licenciadas"], x["status_cobertura"]]
                         for x in T("agg_viagens_periodo").where("nivel = 'mes'").orderBy("periodo").collect()]
r["resp_evolucao"] = [[x["ano_mes"], x["viagens_realizadas"], x["viagens_licenciadas"], x["passageiros_licenca"], x["bilhetes_transportados"],
                       x["bilhetes_vendidos"], x["viagens_realizadas_mom_pct"], x["bilhetes_vendidos_mom_pct"]] for x in ev]
r["resp_qualidade"] = [[x["tabela"], x["regra"], x["violados"], x["avaliados"]] for x in
                       T("agg_qualidade_registros").where("origem in ('quarentena','cobertura')").collect()]
r["ok"] = r["a_ok"] and r["e_ok"] and r["g_ok"]
dbutils.notebook.exit(json.dumps(r, ensure_ascii=False, default=str))
