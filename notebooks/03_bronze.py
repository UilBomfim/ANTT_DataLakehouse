# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Bronze: CSV do Volume para tabelas Delta
# MAGIC
# MAGIC Lê arquivo a arquivo (encoding detectado por arquivo), mantém todas as colunas como STRING,
# MAGIC normaliza o cabeçalho (snake_case sem acento) e faz **append** em `antt_bronze.bronze_<fonte>`.
# MAGIC Idempotente: arquivos cujo `_source_file` já existe na tabela com o mesmo `_file_mod_time` são pulados.
# MAGIC Arquivo republicado (mtime no Volume diferente do gravado): as linhas antigas do arquivo são apagadas e o arquivo é recarregado (B1).
# MAGIC Layouts diferentes entre meses são unidos por nome (colunas ausentes ficam nulas) e listados no resultado.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.text("data_inicio", "2025-10", "Data início (YYYY-MM)")
dbutils.widgets.text("data_fim", "2026-09", "Data fim (YYYY-MM)")
dbutils.widgets.text("fontes", "all", "Tabelas bronze (all ou lista: viagens_fretado,...)")

CATALOG = dbutils.widgets.get("catalog")
DATA_INICIO = dbutils.widgets.get("data_inicio")
DATA_FIM = dbutils.widgets.get("data_fim")
SEL = dbutils.widgets.get("fontes")

VOL_PATH = f"/Volumes/{CATALOG}/antt_raw/landing"
SCHEMA_BRONZE = f"{CATALOG}.antt_bronze"

# tabela bronze -> [(pasta no Volume, valor de tipo_empresa ou None)]
TABELAS = {
    "viagens_fretado": [("viagens_fretado", None)],
    "paradas_fretado": [("paradas_fretado", None)],
    "bilhetes": [("bilhetes", None)],
    "empresas": [("empresas_fretamento", "fretamento"), ("empresas_regular", "regular")],
    "licencas_continuo": [("licencas_continuo", None)],
    "sisaut": [("sisaut", None)],
}
ATIVAS = list(TABELAS) if SEL.strip().lower() in ("", "all") else [s.strip() for s in SEL.split(",")]

# COMMAND ----------

import codecs, csv, json, os, re, unicodedata
from collections import Counter
from datetime import datetime, timezone

from pyspark.sql import functions as F

MESES = {"jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
         "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12}


def ym(t):
    a, m = t.split("-")
    return int(a), int(m)


INI, FIM = ym(DATA_INICIO), ym(DATA_FIM)


def periodo_arquivo(nome):
    arq = nome.lower()
    m = re.search(r"(?<!\d)(\d{2})_(\d{4})(?!\d)", arq)
    if m:
        return int(m.group(2)), int(m.group(1))
    m = re.search(r"(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)(\d{4})", arq)
    if m:
        return int(m.group(2)), MESES[m.group(1)]
    m = re.match(r"(\d{4})_", arq)
    if m:
        return int(m.group(1)), None
    return None


def na_janela(per):
    if per is None:
        return True  # upload manual sem período no nome: carrega
    ano, mes = per
    if mes is None:
        return INI[0] <= ano <= FIM[0]
    return INI <= (ano, mes) <= FIM


def rotulo_periodo(per):
    if per is None:
        return None
    return f"{per[0]}" if per[1] is None else f"{per[0]}-{per[1]:02d}"


def normalizar(nome, i):
    s = (nome or "").replace("\ufeff", "")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s or f"col_{i}"


def normalizar_todas(nomes):
    vistos, saida = Counter(), []
    for i, n in enumerate(nomes):
        c = normalizar(n, i)
        vistos[c] += 1
        saida.append(c if vistos[c] == 1 else f"{c}_{vistos[c]}")
    return saida


def detectar_encoding(caminho):
    """utf-8 se o arquivo inteiro decodifica como utf-8, senão latin1."""
    dec = codecs.getincrementaldecoder("utf-8")()
    try:
        with open(caminho, "rb") as f:
            while True:
                bloco = f.read(8 * 1024 * 1024)
                if not bloco:
                    dec.decode(b"", final=True)
                    break
                dec.decode(bloco)
        return "utf-8"
    except UnicodeDecodeError:
        return "latin1"


def ler_cabecalho(caminho, enc):
    with open(caminho, "r", encoding=enc, newline="") as f:
        linha = f.readline()
    return next(csv.reader([linha.rstrip("\r\n")], delimiter=";", quotechar='"'), [])


SPARK_ENC = {"utf-8": "UTF-8", "latin1": "ISO-8859-1"}

# COMMAND ----------

# MAGIC %md ## Inventário dos arquivos (encoding e cabeçalho de todos os arquivos na janela)

# COMMAND ----------

inventario = {}   # tabela -> lista de dicts
for tabela in ATIVAS:
    itens = []
    for pasta, tipo in TABELAS[tabela]:
        d = f"{VOL_PATH}/{pasta}"
        if not os.path.isdir(d):
            continue
        for nome in sorted(os.listdir(d)):
            if not nome.lower().endswith(".csv"):
                continue
            per = periodo_arquivo(nome)
            if not na_janela(per):
                continue
            caminho = f"{d}/{nome}"
            enc = detectar_encoding(caminho)
            cols = normalizar_todas(ler_cabecalho(caminho, enc))
            itens.append({"caminho": caminho, "arquivo": nome, "pasta": pasta, "tipo_empresa": tipo,
                          "periodo": rotulo_periodo(per), "encoding": enc, "colunas": cols,
                          "bytes": os.path.getsize(caminho)})
    inventario[tabela] = itens
    print(f"[{tabela}] {len(itens)} arquivos na janela | encodings: {dict(Counter(i['encoding'] for i in itens))}")

# COMMAND ----------

# MAGIC %md ## Carga incremental (append por arquivo)

# COMMAND ----------

def carregar_arquivo(item, tabela_fq, colunas_tabela):
    enc = SPARK_ENC[item["encoding"]]
    df = (spark.read.option("header", True).option("sep", ";").option("quote", '"').option("escape", '"')
          .option("encoding", enc).option("multiLine", False).option("inferSchema", False)
          .csv(item["caminho"]))
    nomes = normalizar_todas(df.columns)
    df = df.toDF(*nomes).select([F.col(f"`{c}`").cast("string").alias(c) for c in nomes])
    if item["tipo_empresa"]:
        df = df.withColumn("tipo_empresa", F.lit(item["tipo_empresa"]))
    negocio = df.columns
    mod = datetime.fromtimestamp(os.path.getmtime(item["caminho"]), tz=timezone.utc)
    df = (df
          .withColumn("_row_hash", F.sha2(F.concat_ws("\x1f", *[F.coalesce(F.col(f"`{c}`"), F.lit("")) for c in negocio]), 256))
          .withColumn("_source_file", F.lit(item["caminho"]))
          .withColumn("_file_mod_time", F.lit(mod.isoformat()).cast("timestamp"))
          .withColumn("_ingest_ts", F.current_timestamp())
          .withColumn("_fonte", F.lit(item["pasta"])))
    meta = ["_row_hash", "_source_file", "_file_mod_time", "_ingest_ts", "_fonte"]
    # unionByName com allowMissingColumns: completa o que falta com nulos e mantém a ordem da tabela
    if colunas_tabela:
        faltam = [c for c in colunas_tabela if c not in df.columns]
        for c in faltam:
            df = df.withColumn(c, F.lit(None).cast("string"))
        novas = [c for c in df.columns if c not in colunas_tabela]
        df = df.select(colunas_tabela + novas)
    else:
        df = df.select([c for c in df.columns if c not in meta] + meta)
    return df


carregados = {}
for tabela in ATIVAS:
    tfq = f"{SCHEMA_BRONZE}.bronze_{tabela}"
    existe = spark.catalog.tableExists(tfq)
    ja = {}
    if existe:
        # epoch em segundos: evita ambiguidade de fuso ao comparar com o mtime do Volume
        ja = {r["_source_file"]: r["mod"] for r in
              spark.table(tfq).groupBy("_source_file").agg(F.max(F.col("_file_mod_time").cast("double")).alias("mod")).collect()}

    def republicado(item):
        """B1: arquivo já carregado cujo mtime no Volume difere do gravado (o 02_ingest rebaixou uma versão nova)."""
        gravado = ja.get(item["caminho"])
        if gravado is None:
            return False
        return gravado is not None and abs(gravado - os.path.getmtime(item["caminho"])) > 1

    novos = [i for i in inventario[tabela] if i["caminho"] not in ja]
    recarregar = [i for i in inventario[tabela] if i["caminho"] in ja and republicado(i)]
    for item in recarregar:
        # apaga as linhas da versão antiga do arquivo e recarrega (só este arquivo)
        spark.sql(f"DELETE FROM {tfq} WHERE _source_file = '{item['caminho'].replace(chr(39), chr(39) * 2)}'")
        print(f"[{tabela}] arquivo republicado, linhas antigas apagadas: {item['arquivo']}")
    novos = novos + recarregar
    carregados[tabela] = []
    for item in novos:
        cols_tab = spark.table(tfq).columns if spark.catalog.tableExists(tfq) else None
        df = carregar_arquivo(item, tfq, cols_tab)
        (df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(tfq))
        carregados[tabela].append(item["arquivo"])
        print(f"[{tabela}] carregado {item['arquivo']} ({item['encoding']})")
    print(f"[{tabela}] {len(novos)} carregados ({len(recarregar)} republicados), {len(inventario[tabela]) - len(novos)} já carregados")

# COMMAND ----------

# MAGIC %md ## Resumo: contagens, divergências de layout e encodings

# COMMAND ----------

contagem_tabela, contagem_arquivo, divergencias, encodings = {}, {}, {}, {}
for tabela in ATIVAS:
    tfq = f"{SCHEMA_BRONZE}.bronze_{tabela}"
    if not spark.catalog.tableExists(tfq):
        contagem_tabela[tabela] = 0
        contagem_arquivo[tabela] = {}
        continue
    por_arq = {r["_source_file"]: r["n"] for r in
               spark.table(tfq).groupBy("_source_file").agg(F.count("*").alias("n")).collect()}
    contagem_arquivo[tabela] = {os.path.basename(k): v for k, v in sorted(por_arq.items())}
    contagem_tabela[tabela] = sum(por_arq.values())

    itens = inventario[tabela]
    encodings[tabela] = {i["arquivo"]: i["encoding"] for i in itens}
    # layout de referência = o mais frequente (empate: o mais recente)
    grupos = Counter(tuple(i["colunas"]) for i in itens)
    div = []
    if grupos:
        base = list(max(grupos.items(), key=lambda kv: kv[1])[0])
        for i in itens:
            if i["colunas"] != base:
                div.append({"arquivo": i["arquivo"], "periodo": i["periodo"],
                            "colunas_a_mais": [c for c in i["colunas"] if c not in base],
                            "colunas_a_menos": [c for c in base if c not in i["colunas"]],
                            "ordem_diferente": set(i["colunas"]) == set(base)})
    divergencias[tabela] = {"layout_referencia": base if grupos else [], "arquivos_divergentes": div}

periodo_por_arquivo = {i["arquivo"]: i["periodo"] for t in inventario.values() for i in t}
linhas_por_periodo = {}
for tabela, d in contagem_arquivo.items():
    for arq, n in d.items():
        p = periodo_por_arquivo.get(arq)
        linhas_por_periodo.setdefault(tabela, {})
        linhas_por_periodo[tabela][p] = linhas_por_periodo[tabela].get(p, 0) + n

resultado = {
    "janela": [DATA_INICIO, DATA_FIM],
    "arquivos_novos_carregados": {t: len(v) for t, v in carregados.items()},
    "linhas_por_tabela": contagem_tabela,
    "linhas_por_arquivo": contagem_arquivo,
    "linhas_por_periodo": linhas_por_periodo,
    "encodings": encodings,
    "bytes_por_arquivo": {i["arquivo"]: i["bytes"] for t in inventario.values() for i in t},
    "divergencias_colunas": divergencias,
}
saida = json.dumps(resultado, ensure_ascii=False, default=str)
os.makedirs(f"{VOL_PATH}/_resultados", exist_ok=True)
with open(f"{VOL_PATH}/_resultados/03_bronze.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(resultado, ensure_ascii=False, indent=2, default=str))
print(json.dumps({"linhas_por_tabela": contagem_tabela, "novos": resultado["arquivos_novos_carregados"]}, ensure_ascii=False))
dbutils.notebook.exit(saida)
