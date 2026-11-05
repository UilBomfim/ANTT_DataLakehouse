# Databricks notebook source
# MAGIC %md
# MAGIC # 95 - Verificação das consultas de `08_analise.sql`
# MAGIC Lê o arquivo SQL do Volume (`_resultados/08_analise.sql`), separa as células por `-- COMMAND ----------`, executa cada consulta e devolve
# MAGIC um resumo pequeno: título, número de linhas e erro (se houver). O arquivo é copiado para o Volume pelo script `validar_consultas.ps1`.

# COMMAND ----------

import json

CAMINHO = "/Volumes/workspace/antt_raw/landing/_resultados/08_analise.sql"
texto = open(CAMINHO, encoding="utf-8").read()
celulas = [c for c in texto.split("-- COMMAND ----------") if c.strip()]

resultado = []
for i, cel in enumerate(celulas):
    linhas = cel.strip().splitlines()
    if all(l.startswith("-- MAGIC") or l.startswith("-- Databricks") or not l.strip() for l in linhas):
        continue  # célula de texto
    titulo = next((l[2:].strip() for l in linhas if l.startswith("--") and not l.startswith("-- MAGIC")), f"celula {i}")
    sql = "\n".join(l for l in linhas if not l.strip().startswith("--")).strip().rstrip(";")
    item = {"celula": i, "titulo": titulo[:90]}
    try:
        df = spark.sql(sql)
        rows = df.limit(1000).collect()
        item["linhas"] = len(rows)
        item["colunas"] = df.columns
        item["amostra"] = [list(map(lambda v: None if v is None else str(v)[:40], r)) for r in rows[:2]]
    except Exception as e:
        item["erro"] = str(e)[:400]
    resultado.append(item)

erros = [r for r in resultado if "erro" in r]
vazias = [r for r in resultado if r.get("linhas") == 0]
resumo = {"consultas": len(resultado), "com_erro": len(erros), "vazias": len(vazias),
          "erros": erros, "vazias_titulos": [r["titulo"] for r in vazias],
          "linhas_por_consulta": [[r["titulo"], r.get("linhas")] for r in resultado]}
dbutils.notebook.exit(json.dumps(resumo, ensure_ascii=False))
