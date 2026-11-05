# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Silver: resultados de qualidade (`silver.dq_resultados`)
# MAGIC
# MAGIC Consolida as regras de todas as tabelas Silver e das quarentenas em uma tabela:
# MAGIC `execucao_ts, tabela, regra, severidade, avaliados, violados, pct`.
# MAGIC
# MAGIC Severidade: `critica` (registro vai para quarentena/é descartado ou a chave é violada), `alerta` (registro fica, sinalizado),
# MAGIC `info` (observação de cobertura/plausibilidade).
# MAGIC
# MAGIC Rodar por último (depende de todas as tabelas Silver).
# MAGIC
# MAGIC Escrita: overwrite com a execução mais recente. Guardar histórico por append duplicaria linhas a cada reexecução; o histórico
# MAGIC fica para a camada de orquestração (um job agendado grava um snapshot por execução). Reexecutar não duplica.

# COMMAND ----------

import json
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql import types as T

dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.text("data_inicio", "2025-10", "Data início (YYYY-MM)")
dbutils.widgets.text("data_fim", "2026-09", "Data fim (YYYY-MM)")
CATALOG = dbutils.widgets.get("catalog")
B = f"{CATALOG}.antt_bronze"
S = f"{CATALOG}.antt_silver"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {S}")
EXEC_TS = datetime.now()

linhas = []


def regras(tabela, df, lista, avaliados=None):
    """lista = [(regra, severidade, expressão booleana de violação)]; uma varredura por tabela."""
    aggs = [F.count(F.lit(1)).alias("_n")] + [
        F.sum(F.when(F.coalesce(expr, F.lit(False)), 1).otherwise(0)).alias(f"r{i}") for i, (_, _, expr) in enumerate(lista)]
    row = df.agg(*aggs).collect()[0]
    n = int(avaliados if avaliados is not None else row["_n"])
    for i, (regra, sev, _) in enumerate(lista):
        viol = int(row[f"r{i}"] or 0)
        linhas.append((EXEC_TS, tabela, regra, sev, n, viol, round(100.0 * viol / n, 4) if n else None))


def chave_duplicada(tabela, df, chave):
    d = df.groupBy(*chave).count().where("count > 1").agg(F.sum(F.col("count") - 1)).collect()[0][0] or 0
    linhas.append((EXEC_TS, tabela, f"chave_unica({'+'.join(chave)})", "critica", df.count(), int(d), None))
    n = linhas[-1][4]
    linhas[-1] = linhas[-1][:6] + (round(100.0 * int(d) / n, 4) if n else None,)


def flag(f):
    return F.array_contains("dq_flags", f)


# COMMAND ----------

emp = spark.table(f"{S}.empresa")
regras("empresa", emp, [("cnpj_digito_verificador_invalido", "alerta", ~F.col("cnpj_valido")),
                        ("cnpj_nao_14_digitos", "critica", F.length("cnpj") != 14)])
chave_duplicada("empresa", emp, ["cnpj"])
eh = spark.table(f"{S}.empresa_historico")
regras("empresa_historico", eh, [("cnpj_digito_verificador_invalido", "alerta", ~F.col("cnpj_valido"))])
chave_duplicada("empresa_historico", eh, ["cnpj", "snapshot_mes", "tipo_empresa"])

cob = spark.table(f"{S}.cobertura_fonte_mes")
regras("cobertura_fonte_mes", cob, [("mes_ausente", "info", F.col("status") == "ausente"),
                                    ("mes_parcial", "info", F.col("status") == "parcial")])
chave_duplicada("cobertura_fonte_mes", cob, ["fonte", "ano_mes"])

lic = spark.table(f"{S}.licenca")
regras("licenca", lic, [
    ("cnpj_nulo", "critica", F.col("cnpj").isNull()),
    ("uf_invalida", "alerta", ~F.coalesce(F.col("uf_valida"), F.lit(False))),
    ("total_passageiros_fora_de_1_a_99", "alerta", ~F.col("total_passageiros").between(1, 99)),
    ("total_passageiros_nao_constante_na_licenca", "alerta", F.col("pax_nao_constante")),
    ("data_fim_antes_data_inicio", "alerta", F.col("data_fim") < F.col("data_inicio")),
    ("contratante_texto_corrompido_nao_reparado", "alerta", F.col("contratante_texto") == "invalido"),
    ("cpf_completo_exposto", "critica", F.col("cnpj_cpf_contratante").rlike(r"^\d{11}$") | F.col("cnpj_cpf_contratante").rlike(r"^\d{3}\.\d{3}\.\d{3}-\d{2}$")),
])
chave_duplicada("licenca", lic, ["licenca_id", "origem_registro"])

tr = spark.table(f"{S}.licenca_trecho")
regras("licenca_trecho", tr, [
    ("uf_inicio_invalida", "alerta", ~F.coalesce(F.col("uf_inicio_valida"), F.lit(False))),
    ("uf_final_invalida", "alerta", ~F.coalesce(F.col("uf_final_valida"), F.lit(False))),
    ("contratante_corrompido_nao_reparado", "alerta", F.col("contratante_texto") == "invalido"),
    ("localidade_inicio_corrompida_nao_reparada", "alerta", F.col("localidade_inicio_trecho_texto") == "invalido"),
    ("localidade_final_corrompida_nao_reparada", "alerta", F.col("localidade_final_trecho_texto") == "invalido"),
    ("texto_reparado_contratante", "info", F.col("contratante_texto") == "corrigido"),
    ("texto_reparado_localidade_inicio", "info", F.col("localidade_inicio_trecho_texto") == "corrigido"),
    ("texto_reparado_localidade_final", "info", F.col("localidade_final_trecho_texto") == "corrigido"),
])
chave_duplicada("licenca_trecho", tr, ["licenca_id", "sequencia_trecho"])

vf = spark.table(f"{S}.viagem_fretado")
qv = spark.table(f"{S}.quarentena_viagem_fretado")
regras("viagem_fretado", vf, [
    ("duracao_atipica", "alerta", flag("duracao_atipica")),
    ("empresa_nao_habilitada", "alerta", flag("empresa_nao_habilitada")),
    ("sem_licenca", "alerta", flag("sem_licenca")),
    ("codigo_repetido_entre_arquivos", "alerta", flag("codigo_repetido_entre_arquivos")),
    ("sem_data_fim", "alerta", flag("sem_data_fim")),
])
chave_duplicada("viagem_fretado", vf, ["codigo_viagem"])
n_viagem_total = vf.count() + qv.count()
regras("quarentena_viagem_fretado", qv, [
    ("motivo_codigo_repetido_entre_arquivos", "critica", F.col("motivo") == "codigo_repetido_entre_arquivos"),
    ("motivo_codigo_repetido_mesmo_arquivo", "critica", F.col("motivo") == "codigo_repetido_mesmo_arquivo"),
    ("motivo_fim_antes_do_inicio", "critica", F.col("motivo") == "fim_antes_do_inicio"),
    ("motivo_campo_obrigatorio_nulo", "critica", F.col("motivo") == "campo_obrigatorio_nulo"),
], avaliados=n_viagem_total)

pf = spark.table(f"{S}.parada_fretado")
qp = spark.table(f"{S}.quarentena_parada_fretado")
regras("parada_fretado", pf, [
    ("mes_parcial", "info", F.col("mes_parcial")),
    ("viagem_nao_encontrada", "alerta", F.col("viagem_nao_encontrada")),
    ("data_parada_em_mes_diferente_do_arquivo", "info", F.col("ano_mes_parada") != F.col("ano_mes_arquivo")),
    ("sem_licenca_id", "alerta", F.col("licenca_id").isNull()),
])
chave_duplicada("parada_fretado", pf, ["codigo_viagem", "data_parada", "motivo_parada"])
regras("quarentena_parada_fretado", qp, [("chave_natural_nula_ou_invalida", "critica", F.col("motivo") == "chave_natural_nula_ou_invalida")],
       avaliados=pf.count() + qp.count())

ba = spark.table(f"{S}.bilhete_agregado")
regras("bilhete_agregado", ba, [
    ("mes_viagem_antes_emissao", "alerta", F.col("mes_viagem_antes_emissao")),
    ("mes_viagem_futuro_mais_de_12_meses", "alerta", F.col("mes_viagem_futuro")),
    ("grao_repetido", "alerta", F.col("grao_repetido")),
    ("uf_invalida", "alerta", F.col("uf_invalida")),
    ("quantidade_nula_ou_nao_positiva", "critica", F.col("quantidade_bilhetes").isNull() | (F.col("quantidade_bilhetes") <= 0)),
    ("mes_emissao_ou_viagem_nulo", "critica", F.col("mes_emissao").isNull() | F.col("mes_viagem").isNull()),
])

# COMMAND ----------

schema = T.StructType([
    T.StructField("execucao_ts", T.TimestampType()), T.StructField("tabela", T.StringType()),
    T.StructField("regra", T.StringType()), T.StructField("severidade", T.StringType()),
    T.StructField("avaliados", T.LongType()), T.StructField("violados", T.LongType()), T.StructField("pct", T.DoubleType())])
res = spark.createDataFrame(linhas, schema)
(res.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.dq_resultados"))
spark.sql(f"COMMENT ON TABLE {S}.dq_resultados IS 'Regras de qualidade da Silver: avaliados, violados e pct por tabela e regra (última execução)'")

D = spark.table(f"{S}.dq_resultados")
r = {
    "linhas": D.count(),
    "tabelas": D.select("tabela").distinct().count(),
    "violadas_maior_que_zero": [[x["tabela"], x["regra"], x["severidade"], x["avaliados"], x["violados"], x["pct"]]
                                for x in D.where("violados > 0").orderBy("tabela", "regra").collect()],
    "sem_violacao": [[x["tabela"], x["regra"]] for x in D.where("violados = 0").orderBy("tabela", "regra").collect()],
    "contagem_tabelas": {t: spark.table(f"{S}.{t}").count() for t in
                         ["cobertura_fonte_mes", "empresa", "empresa_historico", "licenca", "licenca_trecho", "viagem_fretado",
                          "quarentena_viagem_fretado", "parada_fretado", "quarentena_parada_fretado", "bilhete_agregado", "dq_resultados"]},
}
print(json.dumps(r, ensure_ascii=False, indent=1, default=str))
dbutils.notebook.exit(json.dumps(r, ensure_ascii=False, default=str))
