# Databricks notebook source
# MAGIC %md
# MAGIC # 96 - Teste do B1 (arquivo republicado)
# MAGIC Tudo dentro do Databricks: (1) conta as linhas de `bronze_empresas`, (2) reescreve 1 CSV pequeno de `empresas_regular` com o mesmo conteúdo (atualiza o mtime, dados idênticos),
# MAGIC (3) roda `03_bronze` para `empresas` (deve apagar e recarregar só esse arquivo, mantendo a contagem), (4) roda de novo (não deve recarregar nada).

# COMMAND ----------

import json, os

T = "workspace.antt_bronze.bronze_empresas"
D = "/Volumes/workspace/antt_raw/landing/empresas_regular"


def estado():
    t = spark.table(T)
    por = {r["arq"]: [r["n"], str(r["mod"])] for r in (
        t.where("tipo_empresa = 'regular'")
         .selectExpr("regexp_extract(_source_file, '([^/]+)$', 1) as arq", "_file_mod_time as mod")
         .groupBy("arq", "mod").count().withColumnRenamed("count", "n").collect())}
    return {"linhas": t.count(), "regular": por}


def resumo_bronze(txt):
    r = json.loads(txt)
    return {"novos_empresas": r["arquivos_novos_carregados"].get("empresas"),
            "linhas_empresas": r["linhas_por_tabela"].get("empresas")}


out = {}
out["antes"] = estado()

# COMMAND ----------

import hashlib, time

arq = sorted(f for f in os.listdir(D) if f.lower().endswith(".csv"))[0]
p = f"{D}/{arq}"
antes = os.path.getmtime(p)
# os.utime não funciona em Volumes (mtime não muda). Reescrever o arquivo com o MESMO conteúdo atualiza o mtime sem alterar dados.
conteudo = open(p, "rb").read()
hash_antes = hashlib.sha256(conteudo).hexdigest()
time.sleep(2)
with open(p, "wb") as f:
    f.write(conteudo)
depois = os.path.getmtime(p)
hash_depois = hashlib.sha256(open(p, "rb").read()).hexdigest()
out["touch"] = {"arquivo": arq, "mtime_antes": antes, "mtime_depois": depois, "mtime_mudou": depois != antes,
                "conteudo_identico": hash_antes == hash_depois, "bytes": len(conteudo)}

# COMMAND ----------

out["bronze_1"] = resumo_bronze(dbutils.notebook.run("./03_bronze", 3000, {"fontes": "empresas"}))
out["apos_bronze_1"] = estado()

# COMMAND ----------

out["bronze_2"] = resumo_bronze(dbutils.notebook.run("./03_bronze", 3000, {"fontes": "empresas"}))
out["apos_bronze_2"] = estado()

# COMMAND ----------

a, b, c = out["antes"], out["apos_bronze_1"], out["apos_bronze_2"]
out["veredito"] = {
    "contagem_antes_igual_depois_1": a["linhas"] == b["linhas"],
    "contagem_antes_igual_depois_2": a["linhas"] == c["linhas"],
    "primeira_rodada_recarregou_1_arquivo": out["bronze_1"]["novos_empresas"] == 1,
    "segunda_rodada_recarregou_0": out["bronze_2"]["novos_empresas"] == 0,
    "mod_time_do_arquivo_mudou": a["regular"].get(arq, [None, None])[1] != b["regular"].get(arq, [None, None])[1],
}
dbutils.notebook.exit(json.dumps(out, ensure_ascii=False, default=str))
