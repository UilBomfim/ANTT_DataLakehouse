# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Gold: indicadores (as 6 perguntas do roteiro)
# MAGIC
# MAGIC Schema `workspace.antt_gold`. Escrita: **overwrite determinístico** (derivado por completo das dimensões e fatos).
# MAGIC Rodar depois de `05_gold_dimensoes` e `05_gold_fatos`.
# MAGIC
# MAGIC | Tabela | Pergunta | Grão |
# MAGIC |---|---|---|
# MAGIC | `agg_viagens_periodo` | 1. Viagens por período | nivel (dia/semana/mes) × período |
# MAGIC | `agg_ranking_empresas` | 2. Empresas com maior volume | cnpj × período (mes ou total) |
# MAGIC | `agg_origem_destino` | 3. Principais origens e destinos | visao (fretado/regular) × tipo (origem/destino/par) × localidade |
# MAGIC | `agg_duracao_media` | 4. Duração média | nivel (mes/empresa_mes) × período |
# MAGIC | `agg_qualidade_registros` | 5. Duplicados/inconsistentes | tabela × regra |
# MAGIC | `agg_evolucao_mensal_volume` | 6. Evolução mensal do volume | mês |
# MAGIC
# MAGIC Regras comuns: mês sem cobertura (`ausente`) aparece com valor **nulo**, não zero (G1). Variações MoM/YoY só existem quando o
# MAGIC mês e o mês de comparação são completos. Todo indicador mensal traz `status_cobertura` e `mes_completo`.

# COMMAND ----------

import json
from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.text("data_inicio", "2025-10", "Data início (YYYY-MM)")
dbutils.widgets.text("data_fim", "2026-09", "Data fim (YYYY-MM)")
CATALOG = dbutils.widgets.get("catalog")
DATA_INICIO = dbutils.widgets.get("data_inicio")
DATA_FIM = dbutils.widgets.get("data_fim")
S = f"{CATALOG}.antt_silver"
G = f"{CATALOG}.antt_gold"
T = lambda n: spark.table(f"{G}.{n}")


def save(df, name, comment):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{G}.{name}")
    spark.sql(f"COMMENT ON TABLE {G}.{name} IS '{comment}'")


# calendário de meses da janela
meses = (spark.sql(f"select explode(sequence(to_date('{DATA_INICIO}-01'), to_date('{DATA_FIM}-01'), interval 1 month)) as d")
         .select(F.date_format("d", "yyyy-MM").alias("ano_mes")))

cob = T("dim_cobertura").select("fonte", "ano_mes", "status", "mes_completo")


def cobertura(fonte, sufixo):
    return cob.where(F.col("fonte") == fonte).select(
        "ano_mes", F.col("status").alias(f"status_cobertura_{sufixo}"), F.col("mes_completo").alias(f"mes_completo_{sufixo}"))


cob_via, cob_par = cobertura("viagens_fretado", "viagens"), cobertura("paradas_fretado", "paradas")
cob_bil, cob_sis = cobertura("bilhetes", "bilhetes"), cobertura("sisaut", "licencas")


def com_mom_yoy(df, col, completo, janela_ordem="ano_mes"):
    """Variações MoM/YoY só quando o mês e o de comparação são completos (G1)."""
    w = Window.orderBy(janela_ordem)
    prev, prev12 = F.lag(col, 1).over(w), F.lag(col, 12).over(w)
    prev_ok, prev12_ok = F.lag(completo, 1).over(w), F.lag(completo, 12).over(w)
    base = F.when(F.col(completo) & prev_ok & (prev > 0), (F.col(col) - prev) / prev * 100)
    yoy = F.when(F.col(completo) & prev12_ok & (prev12 > 0), (F.col(col) - prev12) / prev12 * 100)
    return df.withColumn(f"{col}_mom_pct", F.round(base, 2)).withColumn(f"{col}_yoy_pct", F.round(yoy, 2))


# COMMAND ----------
# MAGIC %md ## 1. agg_viagens_periodo

# COMMAND ----------

fv = T("fato_viagem").withColumn("dia", F.to_date("data_inicio"))
fl = T("fato_licenca")

# mensal: viagens realizadas (Monitriip) e série de apoio de viagens licenciadas (SISAUT + contínuo)
mensal_v = fv.groupBy("ano_mes_inicio").agg(F.count("*").alias("viagens_realizadas")).withColumnRenamed("ano_mes_inicio", "ano_mes")
mensal_l = fl.groupBy("ano_mes_inicio").agg(F.sum("viagens_licenciadas").alias("viagens_licenciadas")).withColumnRenamed("ano_mes_inicio", "ano_mes")
mes = (meses.join(mensal_v, "ano_mes", "left").join(mensal_l, "ano_mes", "left")
       .join(cob_via, "ano_mes", "left").join(cob_sis, "ano_mes", "left"))
mes = (mes.withColumn("viagens_realizadas", F.when(F.col("mes_completo_viagens"), F.coalesce("viagens_realizadas", F.lit(0))))  # ausente/parcial => nulo
          .withColumn("viagens_licenciadas", F.when(F.col("mes_completo_licencas"), F.coalesce("viagens_licenciadas", F.lit(0)))))
mes = com_mom_yoy(mes, "viagens_realizadas", "mes_completo_viagens")
mes = (mes.select(F.lit("mes").alias("nivel"), F.col("ano_mes").alias("periodo"), "viagens_realizadas", "viagens_licenciadas",
                  "viagens_realizadas_mom_pct", "viagens_realizadas_yoy_pct",
                  F.col("status_cobertura_viagens").alias("status_cobertura"), F.col("mes_completo_viagens").alias("mes_completo"),
                  F.col("status_cobertura_licencas").alias("status_cobertura_licencas")))

# diário e semanal (viagens realizadas); nulo nos meses ausentes não se aplica (não há linhas); a coluna de cobertura vem do mês
dia = (fv.groupBy("dia").agg(F.count("*").alias("viagens_realizadas"))
       .withColumn("ano_mes", F.date_format("dia", "yyyy-MM")).join(cob_via, "ano_mes", "left"))
dia = dia.select(F.lit("dia").alias("nivel"), F.date_format("dia", "yyyy-MM-dd").alias("periodo"), "viagens_realizadas",
                 F.lit(None).cast("long").alias("viagens_licenciadas"),
                 F.lit(None).cast("double").alias("viagens_realizadas_mom_pct"), F.lit(None).cast("double").alias("viagens_realizadas_yoy_pct"),
                 F.col("status_cobertura_viagens").alias("status_cobertura"), F.col("mes_completo_viagens").alias("mes_completo"),
                 F.lit(None).cast("string").alias("status_cobertura_licencas"))
sem = (fv.withColumn("sem", F.concat(F.year(F.date_sub(F.next_day("dia", "Mon"), 7)), F.lit("-S"), F.lpad(F.weekofyear("dia"), 2, "0")))
       .groupBy("sem").agg(F.count("*").alias("viagens_realizadas"), F.min("ano_mes_inicio").alias("ano_mes")).join(cob_via, "ano_mes", "left"))
sem = sem.select(F.lit("semana").alias("nivel"), F.col("sem").alias("periodo"), "viagens_realizadas",
                 F.lit(None).cast("long").alias("viagens_licenciadas"),
                 F.lit(None).cast("double").alias("viagens_realizadas_mom_pct"), F.lit(None).cast("double").alias("viagens_realizadas_yoy_pct"),
                 F.col("status_cobertura_viagens").alias("status_cobertura"), F.col("mes_completo_viagens").alias("mes_completo"),
                 F.lit(None).cast("string").alias("status_cobertura_licencas"))

agg_viagens = mes.unionByName(sem).unionByName(dia)
save(agg_viagens, "agg_viagens_periodo",
     "Pergunta 1. Grao: nivel (mes/semana/dia) x periodo. viagens_realizadas (Monitriip) nulo em mes sem cobertura; viagens_licenciadas (SISAUT+continuo) e serie de apoio com medida diferente")

# COMMAND ----------
# MAGIC %md ## 2. agg_ranking_empresas

# COMMAND ----------

emp = T("dim_empresa").select("cnpj", "razao_social")
base = (fv.select("codigo_viagem", "cnpj", "licenca_id", "ano_mes_inicio", "passageiros_licenca")
        .join(emp, "cnpj", "left")
        .withColumn("razao_social", F.coalesce("razao_social", F.lit("NAO_HABILITADA"))))
# passageiros sem dupla contagem: cada licença conta UMA vez (maior valor declarado), atribuída ao mês da sua primeira viagem.
# Assim a soma dos meses = total, e uma licença com viagens em vários meses não é contada de novo.
pax = (base.where("licenca_id is not null").groupBy("cnpj", "licenca_id")
       .agg(F.min("ano_mes_inicio").alias("ano_mes_inicio"), F.max("passageiros_licenca").alias("pax")))


def ranking(grupo_cols, periodo_expr, periodo_nome):
    v = base.groupBy(*grupo_cols, "razao_social").agg(F.count("*").alias("viagens"))
    p = pax.groupBy(*grupo_cols).agg(F.sum("pax").alias("passageiros"))
    r = v.join(p, grupo_cols, "left").withColumn("passageiros", F.coalesce("passageiros", F.lit(0)))
    w = Window.partitionBy(*([c for c in grupo_cols if c != "cnpj"])).orderBy(F.desc("viagens"), F.asc("cnpj"))
    return (r.withColumn("rank_viagens", F.row_number().over(w))  # desempate determinístico por CNPJ
             .withColumn("periodo_tipo", F.lit(periodo_nome)))


r_mes = ranking(["cnpj", "ano_mes_inicio"], None, "mes").withColumnRenamed("ano_mes_inicio", "periodo")
r_tot = ranking(["cnpj"], None, "total").withColumn("periodo", F.lit("total"))
cols_r = ["periodo_tipo", "periodo", "cnpj", "razao_social", "viagens", "passageiros", "rank_viagens"]
agg_rank = (r_mes.select(*cols_r).unionByName(r_tot.select(*cols_r))
            .join(cob_via.withColumnRenamed("ano_mes", "periodo"), "periodo", "left")
            .withColumnRenamed("status_cobertura_viagens", "status_cobertura").withColumnRenamed("mes_completo_viagens", "mes_completo"))
save(agg_rank, "agg_ranking_empresas",
     "Pergunta 2. Grao: periodo_tipo (mes/total) x periodo x cnpj. passageiros somados 1x por licenca (sem dupla contagem). Empresa fora da dim_empresa = NAO_HABILITADA. rank desempatado por cnpj")

# COMMAND ----------
# MAGIC %md ## 3. agg_origem_destino

# COMMAND ----------

loc = T("dim_localidade").select("localidade_key", "municipio", "uf")


def topo(df, chave, tipo, visao, metrica_viagens, metrica_pax):
    r = df.groupBy(chave).agg(metrica_viagens.alias("viagens"), metrica_pax.alias("passageiros")).withColumnRenamed(chave, "localidade_key")
    return (r.join(loc, "localidade_key", "left").withColumn("tipo", F.lit(tipo)).withColumn("visao", F.lit(visao)))


# fretado: viagens realizadas com origem/destino via licença; passageiros da licença (1x por licença, no grão da fato_licenca)
vl = fv.select("localidade_origem_key", "localidade_destino_key")
f_or = topo(vl, "localidade_origem_key", "origem", "fretado", F.count("*"), F.lit(None).cast("long"))
f_de = topo(vl, "localidade_destino_key", "destino", "fretado", F.count("*"), F.lit(None).cast("long"))
# passageiros declarados por localidade vêm da fato_licenca
fl_or = fl.groupBy("localidade_origem_key").agg(F.sum("total_passageiros").alias("p")).withColumnRenamed("localidade_origem_key", "localidade_key")
fl_de = fl.groupBy("localidade_destino_key").agg(F.sum("total_passageiros").alias("p")).withColumnRenamed("localidade_destino_key", "localidade_key")
f_or = f_or.join(fl_or, "localidade_key", "left").withColumn("passageiros", F.col("p").cast("long")).drop("p")
f_de = f_de.join(fl_de, "localidade_key", "left").withColumn("passageiros", F.col("p").cast("long")).drop("p")

# par O-D fretado
par_f = (fv.where("localidade_origem_key is not null and localidade_destino_key is not null")
         .groupBy("localidade_origem_key", "localidade_destino_key").agg(F.count("*").alias("viagens"))
         .withColumn("localidade_key", F.concat_ws(" -> ", "localidade_origem_key", "localidade_destino_key"))
         .withColumn("municipio", F.lit(None).cast("string")).withColumn("uf", F.lit(None).cast("string"))
         .withColumn("passageiros", F.lit(None).cast("long")).withColumn("tipo", F.lit("par")).withColumn("visao", F.lit("fretado"))
         .select("localidade_key", "viagens", "passageiros", "municipio", "uf", "tipo", "visao"))

# regular: bilhetes por mês da viagem em status confiável
fb = T("fato_bilhete").where("status_mes_viagem = 'confiavel'")
b_or = topo(fb, "localidade_origem_key", "origem", "regular", F.lit(None).cast("long"), F.sum("quantidade_bilhetes").cast("long"))
b_de = topo(fb, "localidade_destino_key", "destino", "regular", F.lit(None).cast("long"), F.sum("quantidade_bilhetes").cast("long"))
par_b = (fb.where("localidade_origem_key is not null and localidade_destino_key is not null")
         .groupBy("localidade_origem_key", "localidade_destino_key").agg(F.sum("quantidade_bilhetes").cast("long").alias("passageiros"))
         .withColumn("localidade_key", F.concat_ws(" -> ", "localidade_origem_key", "localidade_destino_key"))
         .withColumn("viagens", F.lit(None).cast("long"))
         .withColumn("municipio", F.lit(None).cast("string")).withColumn("uf", F.lit(None).cast("string"))
         .withColumn("tipo", F.lit("par")).withColumn("visao", F.lit("regular"))
         .select("localidade_key", "viagens", "passageiros", "municipio", "uf", "tipo", "visao"))

cols_od = ["visao", "tipo", "localidade_key", "municipio", "uf", "viagens", "passageiros"]
od = (f_or.select(*cols_od).unionByName(f_de.select(*cols_od)).unionByName(par_f.select(*cols_od))
      .unionByName(b_or.select(*cols_od)).unionByName(b_de.select(*cols_od)).unionByName(par_b.select(*cols_od)))
vol = F.coalesce(F.col("viagens"), F.col("passageiros"))
w_od = Window.partitionBy("visao", "tipo").orderBy(F.desc(vol), F.asc("localidade_key"))
agg_od = od.withColumn("rank", F.row_number().over(w_od)).where("rank <= 200")
save(agg_od, "agg_origem_destino",
     "Pergunta 3. Grao: visao (fretado/regular) x tipo (origem/destino/par) x localidade, top 200. fretado = viagens realizadas via licenca; regular = bilhetes por mes da viagem com status confiavel")

# COMMAND ----------
# MAGIC %md ## 4. agg_duracao_media

# COMMAND ----------

# G5: exclui duração atípica e a cópia mantida dos códigos repetidos entre arquivos
vd = fv.where("duracao_min is not null and not array_contains(dq_flags, 'duracao_atipica') "
              "and not array_contains(dq_flags, 'codigo_repetido_entre_arquivos')")


def estat(df, grupo):
    return df.groupBy(*grupo).agg(F.count("*").alias("viagens_consideradas"),
                                  F.round(F.avg("duracao_min"), 1).alias("duracao_media_min"),
                                  F.round(F.percentile_approx("duracao_min", 0.5), 1).alias("duracao_mediana_min"),
                                  F.round(F.percentile_approx("duracao_min", 0.9), 1).alias("duracao_p90_min"))


d_mes = (meses.join(estat(vd, ["ano_mes_inicio"]).withColumnRenamed("ano_mes_inicio", "ano_mes"), "ano_mes", "left")
         .join(cob_via, "ano_mes", "left"))
for c in ["viagens_consideradas", "duracao_media_min", "duracao_mediana_min", "duracao_p90_min"]:
    d_mes = d_mes.withColumn(c, F.when(F.col("mes_completo_viagens"), F.col(c)))  # ausente => nulo
d_mes = d_mes.select(F.lit("mes").alias("nivel"), F.col("ano_mes").alias("periodo"), F.lit(None).cast("string").alias("cnpj"),
                     "viagens_consideradas", "duracao_media_min", "duracao_mediana_min", "duracao_p90_min",
                     F.col("status_cobertura_viagens").alias("status_cobertura"), F.col("mes_completo_viagens").alias("mes_completo"))
d_emp = (estat(vd, ["ano_mes_inicio", "cnpj"]).withColumnRenamed("ano_mes_inicio", "ano_mes").join(cob_via, "ano_mes", "left"))
d_emp = d_emp.select(F.lit("empresa_mes").alias("nivel"), F.col("ano_mes").alias("periodo"), "cnpj",
                     "viagens_consideradas", "duracao_media_min", "duracao_mediana_min", "duracao_p90_min",
                     F.col("status_cobertura_viagens").alias("status_cobertura"), F.col("mes_completo_viagens").alias("mes_completo"))
agg_dur = d_mes.unionByName(d_emp)
save(agg_dur, "agg_duracao_media",
     "Pergunta 4. Grao: nivel (mes/empresa_mes) x periodo (x cnpj). Exclui duracao_atipica e codigo_repetido_entre_arquivos. Mes ausente = nulo")

# COMMAND ----------
# MAGIC %md ## 5. agg_qualidade_registros

# COMMAND ----------

dq = spark.table(f"{S}.dq_resultados").select(
    F.lit("regra_dq").alias("origem"), "tabela", "regra", "severidade", "avaliados", "violados", F.col("pct").alias("pct_violado"),
    F.lit(None).cast("string").alias("detalhe"))

# duplicados removidos e quarentenas (contagens explícitas)
qv = spark.table(f"{S}.quarentena_viagem_fretado")
viagens_total = fv.count() + qv.count()
extras = [
    ("quarentena", "viagem_fretado", "copias_descartadas_codigo_repetido", "critica", viagens_total,
     qv.where("motivo like 'codigo_repetido%'").count(), "copias na quarentena (as 57 copias mantidas seguem na fato com flag)"),
    ("quarentena", "viagem_fretado", "copias_mantidas_codigo_repetido", "alerta", viagens_total,
     fv.where("array_contains(dq_flags, 'codigo_repetido_entre_arquivos')").count(), "copias mantidas na fato_viagem, excluidas da duracao media"),
]
sp = spark.table(f"{S}.parada_fretado")
extras += [("cobertura", "cobertura_fonte_mes", f"mes_{s}", "info", cob.count(), cob.where(f"status = '{s}'").count(),
            ",".join(sorted(x["fonte"] + ":" + x["ano_mes"] for x in cob.where(f"status = '{s}'").collect())))
           for s in ["ausente", "parcial"]]
ex = spark.createDataFrame([(o, t, r, sv, int(a), int(v), float(v) * 100.0 / a if a else None, d) for (o, t, r, sv, a, v, d) in extras],
                           "origem string, tabela string, regra string, severidade string, avaliados long, violados long, pct_violado double, detalhe string")
agg_q = dq.unionByName(ex)
# violações por mês de início para a fato de viagens (flags)
por_mes = (fv.select("ano_mes_inicio", F.explode_outer("dq_flags").alias("flag")).where("flag is not null")
           .groupBy("ano_mes_inicio", "flag").agg(F.count("*").alias("violados"))
           .join(fv.groupBy("ano_mes_inicio").agg(F.count("*").alias("avaliados")), "ano_mes_inicio")
           .select(F.lit("flag_por_mes").alias("origem"), F.lit("fato_viagem").alias("tabela"), F.col("flag").alias("regra"),
                   F.lit("alerta").alias("severidade"), "avaliados", "violados",
                   F.round(F.col("violados") * 100.0 / F.col("avaliados"), 3).alias("pct_violado"), F.col("ano_mes_inicio").alias("detalhe")))
agg_q = agg_q.unionByName(por_mes)
save(agg_q, "agg_qualidade_registros",
     "Pergunta 5. Regras de qualidade da Silver (dq_resultados), quarentenas, cobertura (meses ausente/parcial) e flags da fato_viagem por mes")

# COMMAND ----------
# MAGIC %md ## 6. agg_evolucao_mensal_volume

# COMMAND ----------

fb_all = T("fato_bilhete")
# volume transportado por MÊS DA VIAGEM; vendas por MÊS DE EMISSÃO (séries separadas)
b_trans = (fb_all.groupBy("ano_mes_viagem").agg(F.sum("quantidade_bilhetes").cast("long").alias("bilhetes_transportados"),
                                                  F.first("status_mes_viagem").alias("status_mes_viagem"))
           .withColumnRenamed("ano_mes_viagem", "ano_mes"))
b_trans = (fb_all.groupBy("ano_mes_viagem", "status_mes_viagem").agg(F.sum("quantidade_bilhetes").cast("long").alias("q"))
           .groupBy("ano_mes_viagem").agg(F.sum("q").alias("bilhetes_transportados"),
                                          F.max_by("status_mes_viagem", "q").alias("status_mes_viagem"))
           .withColumnRenamed("ano_mes_viagem", "ano_mes"))
b_vend = fb_all.groupBy("ano_mes_emissao").agg(F.sum("quantidade_bilhetes").cast("long").alias("bilhetes_vendidos")).withColumnRenamed("ano_mes_emissao", "ano_mes")
pax_mes = pax.groupBy("ano_mes_inicio").agg(F.sum("pax").alias("passageiros_licenca")).withColumnRenamed("ano_mes_inicio", "ano_mes")

ev = (meses.join(mensal_v, "ano_mes", "left").join(mensal_l, "ano_mes", "left").join(pax_mes, "ano_mes", "left")
      .join(b_trans, "ano_mes", "left").join(b_vend, "ano_mes", "left")
      .join(cob_via, "ano_mes", "left").join(cob_sis, "ano_mes", "left").join(cob_bil, "ano_mes", "left"))
ev = (ev.withColumn("viagens_realizadas", F.when(F.col("mes_completo_viagens"), F.coalesce("viagens_realizadas", F.lit(0))))
        .withColumn("passageiros_licenca", F.when(F.col("mes_completo_viagens"), F.col("passageiros_licenca")))
        .withColumn("viagens_licenciadas", F.when(F.col("mes_completo_licencas"), F.coalesce("viagens_licenciadas", F.lit(0))))
        .withColumn("mes_completo_transporte_bilhetes", F.col("status_mes_viagem") == "confiavel")
        .withColumn("bilhetes_transportados", F.when(F.col("status_mes_viagem") == "confiavel", F.col("bilhetes_transportados")))
        .withColumn("bilhetes_vendidos", F.when(F.col("mes_completo_bilhetes"), F.col("bilhetes_vendidos"))))
ev = com_mom_yoy(ev, "viagens_realizadas", "mes_completo_viagens")
ev = com_mom_yoy(ev, "viagens_licenciadas", "mes_completo_licencas")
ev = com_mom_yoy(ev, "bilhetes_transportados", "mes_completo_transporte_bilhetes")
ev = com_mom_yoy(ev, "bilhetes_vendidos", "mes_completo_bilhetes")
save(ev.orderBy("ano_mes"), "agg_evolucao_mensal_volume",
     "Pergunta 6. Grao: mes. Series separadas: viagens_realizadas (Monitriip), viagens_licenciadas (SISAUT+continuo), passageiros_licenca, bilhetes_transportados (mes da viagem, so status confiavel) e bilhetes_vendidos (mes de emissao). MoM/YoY nulos sem mes completo; YoY nulo na janela de 12 meses")

# COMMAND ----------

nomes = ["agg_viagens_periodo", "agg_ranking_empresas", "agg_origem_destino", "agg_duracao_media", "agg_qualidade_registros",
         "agg_evolucao_mensal_volume"]
r = {"contagens": {n: T(n).count() for n in nomes}}
fev = {x["nivel"]: [x["viagens_realizadas"], x["viagens_licenciadas"], x["status_cobertura"]]
       for x in T("agg_viagens_periodo").where("periodo = '2026-02' and nivel = 'mes'").collect()}
r["fev2026_viagens_periodo"] = fev
r["fev2026_evolucao"] = [[x["viagens_realizadas"], x["viagens_licenciadas"], x["status_cobertura_viagens"]]
                         for x in T("agg_evolucao_mensal_volume").where("ano_mes = '2026-02'").collect()]
dbutils.notebook.exit(json.dumps(r, ensure_ascii=False, default=str))
