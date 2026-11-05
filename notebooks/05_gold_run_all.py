# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Gold: dimensões e fatos (orquestrador)
# MAGIC Executa em ordem: `05_gold_dimensoes` → `05_gold_fatos` → verificação (`95_verificacao_gold`) → reexecuta dimensões e fatos e verifica de novo.
# MAGIC Devolve só as contagens de cada rodada, para provar a idempotência (as contagens da 2ª rodada devem ser iguais às da 1ª), e o resumo da verificação.

# COMMAND ----------

import json

TIMEOUT = 3000


def rodar(nome):
    return json.loads(dbutils.notebook.run(f"./{nome}", TIMEOUT))


out = {}
out["dim_1"] = rodar("05_gold_dimensoes")
out["fatos_1"] = rodar("05_gold_fatos")
out["verif_1"] = rodar("95_verificacao_gold")
out["dim_2"] = rodar("05_gold_dimensoes")
out["fatos_2"] = rodar("05_gold_fatos")
out["verif_2"] = rodar("95_verificacao_gold")

v1, v2 = out["verif_1"]["a_contagens"], out["verif_2"]["a_contagens"]
out["b_idempotencia"] = {"contagens_iguais": v1 == v2, "rodada_1": v1, "rodada_2": v2}
dbutils.notebook.exit(json.dumps(out, ensure_ascii=False, default=str))
