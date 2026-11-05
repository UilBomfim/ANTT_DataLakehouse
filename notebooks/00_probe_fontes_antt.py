# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Teste de acesso e perfil das fontes ANTT
# MAGIC
# MAGIC Objetivo: verificar se o Databricks Free consegue acessar o portal de dados abertos da ANTT,
# MAGIC baixar uma amostra de cada conjunto e levantar o perfil dos arquivos (colunas, separador,
# MAGIC encoding, nulos, exemplos). O resultado orienta o mapeamento de colunas das camadas Bronze/Silver.
# MAGIC
# MAGIC **Como usar**
# MAGIC 1. Importe este arquivo no workspace (Workspace > Import) e anexe ao compute serverless.
# MAGIC 2. Ajuste os widgets se quiser (catálogo, schema, tamanho máximo de download).
# MAGIC 3. Rode todas as células (Run all).
# MAGIC 4. Copie a saída JSON da última célula, ou baixe o arquivo `perfil_fontes.json` do Volume, e traga para o projeto.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.text("schema", "antt_raw", "Schema")
dbutils.widgets.text("volume", "landing", "Volume")
dbutils.widgets.text("max_mb", "300", "Tamanho máximo por arquivo (MB)")
dbutils.widgets.text("arquivos_por_conjunto", "1", "Arquivos mais recentes por conjunto")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")
MAX_BYTES = int(dbutils.widgets.get("max_mb")) * 1024 * 1024
N_ARQUIVOS = int(dbutils.widgets.get("arquivos_por_conjunto"))

BASE_API = "https://dados.antt.gov.br/api/3/action"
VOL_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# Slugs prováveis no CKAN da ANTT. Se nenhum existir, o notebook procura pelo título.
CONJUNTOS = {
    "viagens_fretado": {
        "slugs": ["monitriip-servico-fretado-viagens"],
        "busca": "Monitriip Serviço Fretado Viagens",
    },
    "paradas_fretado": {
        "slugs": ["monitriip-servico-fretado-paradas"],
        "busca": "Monitriip Serviço Fretado Paradas",
    },
    "bilhetes": {
        "slugs": ["monitriip-bilhetes-de-passagem"],
        "busca": "Monitriip Bilhetes de Passagem",
    },
    "empresas": {
        "slugs": ["empresas-habilitadas"],
        "busca": "Empresas Habilitadas",
        "excluir_recurso": "Semiurbano|TRIC|Multimodal|Peru",  # só Regular/Fretamento
    },
    "licencas_continuo": {
        "slugs": ["autorizacoes-de-viagem-fretado-continuo"],
        "busca": "Licenças de Viagens do Fretamento Contínuo",
    },
    "sisaut": {
        "slugs": ["sisaut-nacional-internacional"],
        "busca": "Licenças de Viagens do Fretamento Eventual e Turístico",
    },
}

# COMMAND ----------

# MAGIC %md ## 1. Teste de conectividade

# COMMAND ----------

import requests

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0 (databricks-antt-probe)"})

conectividade = {}
for nome, url in {
    "antt_https_api": f"{BASE_API}/status_show",
    "antt_https_home": "https://dados.antt.gov.br/",
    "dados_gov_br": "https://dados.gov.br/",
}.items():
    try:
        r = session.get(url, timeout=30)
        conectividade[nome] = {"ok": r.ok, "status": r.status_code}
    except Exception as e:
        conectividade[nome] = {"ok": False, "erro": f"{type(e).__name__}: {e}"[:300]}

print(conectividade)
if not conectividade["antt_https_api"]["ok"]:
    import json as _json
    print(
        "\nATENÇÃO: o Databricks não conseguiu acessar a API da ANTT. "
        "A alternativa é subir os arquivos manualmente para o Volume."
    )
    dbutils.notebook.exit(_json.dumps({"etapa": "conectividade", "conectividade": conectividade}, ensure_ascii=False))

# COMMAND ----------

# MAGIC %md ## 2. Localizar os conjuntos e listar os recursos

# COMMAND ----------

def ckan(action, **params):
    r = session.get(f"{BASE_API}/{action}", params=params, timeout=60)
    r.raise_for_status()
    body = r.json()
    if not body.get("success"):
        raise RuntimeError(body.get("error"))
    return body["result"]


def localizar_pacote(cfg):
    for slug in cfg["slugs"]:
        try:
            return ckan("package_show", id=slug)
        except Exception:
            pass
    resultado = ckan("package_search", q=cfg["busca"], rows=5)
    if resultado["count"] == 0:
        return None
    return resultado["results"][0]


pacotes = {}
erros_localizacao = {}
for chave, cfg in CONJUNTOS.items():
    try:
        pkg = localizar_pacote(cfg)
    except Exception as e:
        pkg = None
        erros_localizacao[chave] = f"{type(e).__name__}: {e}"[:300]
        print(f"[{chave}] erro: {e}")
    if pkg is None:
        erros_localizacao.setdefault(chave, "conjunto não encontrado")
        print(f"[{chave}] conjunto não encontrado")
        continue
    pacotes[chave] = pkg
    print(f"[{chave}] slug={pkg['name']} | título={pkg['title']} | recursos={len(pkg.get('resources', []))}")

# COMMAND ----------

import pandas as pd

linhas = []
for chave, pkg in pacotes.items():
    for res in pkg.get("resources", []):
        linhas.append({
            "conjunto": chave,
            "slug": pkg["name"],
            "recurso": res.get("name"),
            "formato": (res.get("format") or "").lower(),
            "tamanho": res.get("size"),
            "criado": res.get("created"),
            "modificado": res.get("last_modified") or res.get("metadata_modified"),
            "url": res.get("url"),
        })

COLUNAS_RECURSOS = ["conjunto", "slug", "recurso", "formato", "tamanho", "criado", "modificado", "url"]
recursos_pdf = pd.DataFrame(linhas, columns=COLUNAS_RECURSOS)
if recursos_pdf.empty:
    import json as _json
    dbutils.notebook.exit(_json.dumps(
        {"etapa": "localizacao", "conectividade": conectividade, "erros": erros_localizacao},
        ensure_ascii=False))
print(recursos_pdf.to_string(max_colwidth=80))

# COMMAND ----------

# MAGIC %md ## 3. Baixar amostra (arquivos mais recentes de cada conjunto)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

import os, zipfile, hashlib
from urllib.parse import urlparse, unquote

FORMATOS_DADOS = {"csv"}  # CSV mensal; JSON/PDF/HTML ignorados


def nome_arquivo(url):
    return unquote(os.path.basename(urlparse(url).path)) or "arquivo"


def baixar(url, destino):
    with session.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        tamanho = int(r.headers.get("Content-Length") or 0)
        if tamanho and tamanho > MAX_BYTES:
            return {"status": "ignorado_tamanho", "bytes": tamanho}
        h = hashlib.sha256()
        total = 0
        with open(destino, "wb") as f:
            for bloco in r.iter_content(1024 * 1024):
                total += len(bloco)
                if total > MAX_BYTES:
                    f.close()
                    os.remove(destino)
                    return {"status": "ignorado_tamanho", "bytes": total}
                f.write(bloco)
                h.update(bloco)
    return {"status": "baixado", "bytes": total, "sha256": h.hexdigest()}


downloads = []
for chave in pacotes:
    sub = recursos_pdf[(recursos_pdf.conjunto == chave) & (recursos_pdf.formato.isin(FORMATOS_DADOS))]
    excluir = CONJUNTOS[chave].get("excluir_recurso")
    if excluir:
        sub = sub[~sub.recurso.fillna("").str.contains(excluir, case=False, regex=True)]
    sub = sub.sort_values(["modificado", "criado"], ascending=False, na_position="last").head(N_ARQUIVOS)
    pasta = f"{VOL_PATH}/{chave}"
    os.makedirs(pasta, exist_ok=True)
    for _, row in sub.iterrows():
        destino = f"{pasta}/{nome_arquivo(row.url)}"
        try:
            info = baixar(row.url, destino)
        except Exception as e:
            info = {"status": "erro", "erro": f"{type(e).__name__}: {e}"[:300]}
        arquivos = []
        if info["status"] == "baixado" and zipfile.is_zipfile(destino):
            with zipfile.ZipFile(destino) as z:
                z.extractall(pasta)
                arquivos = [f"{pasta}/{n}" for n in z.namelist() if not n.endswith("/")]
        elif info["status"] == "baixado":
            arquivos = [destino]
        downloads.append({"conjunto": chave, "recurso": row.recurso, "url": row.url, **info, "arquivos": arquivos})
        print(chave, info["status"], row.url)

# COMMAND ----------

# MAGIC %md ## 4. Perfil dos arquivos

# COMMAND ----------

from pyspark.sql import functions as F


def detectar_formato(caminho):
    with open(caminho, "rb") as f:
        bruto = f.read(64 * 1024)
    encoding = "utf-8"
    try:
        bruto.decode("utf-8")
    except UnicodeDecodeError:
        encoding = "latin1"
    texto = bruto.decode(encoding, errors="replace")
    if texto.startswith("\ufeff"):
        texto = texto[1:]
    cabecalho = texto.splitlines()[0] if texto else ""
    sep = max([";", ",", "\t", "|"], key=cabecalho.count)
    return encoding, sep, cabecalho, texto.splitlines()[1:4]


def perfilar(caminho):
    encoding, sep, cabecalho, linhas_exemplo = detectar_formato(caminho)
    df = (spark.read.option("header", True).option("sep", sep).option("encoding", encoding)
          .option("multiLine", False).option("inferSchema", False).csv(caminho))
    total = df.count()
    nulos = df.select([
        F.sum(F.when(F.col(f"`{c}`").isNull() | (F.trim(F.col(f"`{c}`")) == ""), 1).otherwise(0)).alias(c)
        for c in df.columns
    ]).collect()[0].asDict()
    distintos = df.select([F.approx_count_distinct(F.col(f"`{c}`")).alias(c) for c in df.columns]).collect()[0].asDict()
    exemplos = [r.asDict() for r in df.limit(5).collect()]
    return {
        "arquivo": caminho,
        "encoding": encoding,
        "separador": sep,
        "cabecalho_bruto": cabecalho,
        "linhas_brutas_exemplo": linhas_exemplo,
        "total_linhas": total,
        "colunas": [
            {"nome": c, "nulos_ou_vazios": nulos[c], "pct_nulos": round(100 * nulos[c] / total, 2) if total else None,
             "distintos_aprox": distintos[c]}
            for c in df.columns
        ],
        "exemplos": exemplos,
    }


perfis = []
for d in downloads:
    for caminho in d["arquivos"]:
        if not caminho.lower().endswith((".csv", ".txt")):
            perfis.append({"conjunto": d["conjunto"], "arquivo": caminho, "obs": "formato não-CSV, não perfilado"})
            continue
        try:
            p = perfilar(caminho)
            p["conjunto"] = d["conjunto"]
            perfis.append(p)
            print(f"[{d['conjunto']}] {caminho}: {p['total_linhas']} linhas, {len(p['colunas'])} colunas, sep='{p['separador']}', {p['encoding']}")
        except Exception as e:
            perfis.append({"conjunto": d["conjunto"], "arquivo": caminho, "erro": f"{type(e).__name__}: {e}"[:500]})

# COMMAND ----------

# MAGIC %md ## 5. Resultado consolidado
# MAGIC Copie a saída abaixo (ou baixe `perfil_fontes.json` do Volume) e traga para o projeto em `dev-out/data/perfil_fontes.json`.

# COMMAND ----------

import json

resultado = {
    "conectividade": conectividade,
    "conjuntos": {
        k: {"slug": p["name"], "titulo": p["title"], "qtd_recursos": len(p.get("resources", []))}
        for k, p in pacotes.items()
    },
    "recursos": recursos_pdf.to_dict(orient="records"),
    "downloads": downloads,
    "perfis": perfis,
}

saida = json.dumps(resultado, ensure_ascii=False, indent=2, default=str)
with open(f"{VOL_PATH}/perfil_fontes.json", "w", encoding="utf-8") as f:
    f.write(saida)
print(f"Salvo em {VOL_PATH}/perfil_fontes.json ({len(saida)} caracteres)")
print(saida)

# COMMAND ----------

resumo = {
    "etapa": "concluido",
    "arquivo_resultado": f"{VOL_PATH}/perfil_fontes.json",
    "conectividade": conectividade,
    "conjuntos": resultado["conjuntos"],
    "erros_localizacao": erros_localizacao,
    "downloads": [{k: d.get(k) for k in ("conjunto", "status", "bytes", "url", "arquivos", "erro")} for d in downloads],
}
dbutils.notebook.exit(json.dumps(resumo, ensure_ascii=False, default=str))
