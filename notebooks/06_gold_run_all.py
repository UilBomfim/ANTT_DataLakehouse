# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Gold: indicadores (orquestrador)
# MAGIC Executa `06_gold_indicadores` duas vezes (a 2ª prova a idempotência: contagens iguais) e a verificação `95_verificacao_gold_agg` após cada rodada.
# MAGIC Devolve só o resumo.

# COMMAND ----------

import json

TIMEOUT = 3000


def rodar(nome):
    return json.loads(dbutils.notebook.run(f"./{nome}", TIMEOUT))


out = {"ind_1": rodar("06_gold_indicadores"), "verif_1": rodar("95_verificacao_gold_agg")}
out["ind_2"] = rodar("06_gold_indicadores")
v2 = rodar("95_verificacao_gold_agg")
out["b_idempotencia"] = {"contagens_iguais": out["ind_1"]["contagens"] == out["ind_2"]["contagens"],
                         "rodada_1": out["ind_1"]["contagens"], "rodada_2": out["ind_2"]["contagens"]}
out["verif_2_ok"] = v2["ok"]
dbutils.notebook.exit(json.dumps(out, ensure_ascii=False, default=str))
