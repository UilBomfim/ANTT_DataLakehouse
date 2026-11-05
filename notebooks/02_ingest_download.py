# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Ingestão: CKAN da ANTT para o Volume landing
# MAGIC
# MAGIC Consulta `package_show` de cada conjunto, filtra os recursos CSV dentro da janela
# MAGIC (`data_inicio` a `data_fim`, formato `YYYY-MM`), baixa em streaming para
# MAGIC `/Volumes/<catalog>/antt_raw/landing/<fonte>/<arquivo>` com retentativa e backoff e registra
# MAGIC cada download em `antt_raw.ingest_manifest`. Arquivo já baixado com o mesmo `last_modified` é pulado.
# MAGIC Se o CKAN estiver fora do ar, o notebook segue com os arquivos que já estão no Volume.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.text("data_inicio", "2025-10", "Data início (YYYY-MM)")
dbutils.widgets.text("data_fim", "2026-09", "Data fim (YYYY-MM)")
dbutils.widgets.text("fontes", "all", "Fontes (all ou lista separada por vírgula)")
dbutils.widgets.text("tentativas", "4", "Tentativas por requisição/arquivo")

CATALOG = dbutils.widgets.get("catalog")
DATA_INICIO = dbutils.widgets.get("data_inicio")
DATA_FIM = dbutils.widgets.get("data_fim")
FONTES_SEL = dbutils.widgets.get("fontes")
TENTATIVAS = int(dbutils.widgets.get("tentativas"))

BASE_API = "https://dados.antt.gov.br/api/3/action"
VOL_PATH = f"/Volumes/{CATALOG}/antt_raw/landing"
MANIFESTO = f"{CATALOG}.antt_raw.ingest_manifest"

# slug CKAN, tipo de periodicidade e filtro por nome do recurso
FONTES = {
    "viagens_fretado": {"slug": "monitriip-servico-fretado-viagens", "periodo": "mensal"},
    "paradas_fretado": {"slug": "monitriip-servico-fretado-paradas", "periodo": "mensal"},
    "bilhetes": {"slug": "monitriip-bilhetes-de-passagem", "periodo": "mensal"},
    "empresas_fretamento": {"slug": "empresas-habilitadas", "periodo": "mensal", "nome": r"Empresas Fretamento"},
    "empresas_regular": {"slug": "empresas-habilitadas", "periodo": "mensal", "nome": r"Empresas Regular"},
    "licencas_continuo": {"slug": "autorizacoes-de-viagem-fretado-continuo", "periodo": "anual"},
    "sisaut": {"slug": "sisaut-nacional-internacional", "periodo": "mensal"},
}

if FONTES_SEL.strip().lower() in ("", "all"):
    FONTES_ATIVAS = list(FONTES)
else:
    FONTES_ATIVAS = [f.strip() for f in FONTES_SEL.split(",") if f.strip()]
    desconhecidas = [f for f in FONTES_ATIVAS if f not in FONTES]
    if desconhecidas:
        raise ValueError(f"Fontes desconhecidas: {desconhecidas}")

# COMMAND ----------

import hashlib, json, os, re, time
from datetime import datetime
from urllib.parse import urlparse, unquote

import requests
from pyspark.sql import types as T

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0 (databricks-antt-lakehouse)"})

MESES = {"jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
         "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12}


def ym(texto):
    a, m = texto.split("-")
    return int(a), int(m)


INI, FIM = ym(DATA_INICIO), ym(DATA_FIM)


def com_retentativa(fn, descricao):
    """Executa fn() com backoff exponencial (3s, 6s, 12s...)."""
    ultimo = None
    for i in range(1, TENTATIVAS + 1):
        try:
            return fn()
        except Exception as e:
            ultimo = e
            espera = 3 * 2 ** (i - 1)
            print(f"  tentativa {i}/{TENTATIVAS} falhou em {descricao}: {type(e).__name__}: {str(e)[:150]}")
            if i < TENTATIVAS:
                time.sleep(espera)
    raise ultimo


def package_show(slug):
    def _f():
        r = session.get(f"{BASE_API}/package_show", params={"id": slug}, timeout=60)
        r.raise_for_status()
        body = r.json()
        if not body.get("success"):
            raise RuntimeError(str(body.get("error")))
        return body["result"]
    return com_retentativa(_f, f"package_show {slug}")


def nome_arquivo(url):
    return unquote(os.path.basename(urlparse(url).path)) or "arquivo"


def periodo_recurso(nome, url):
    """Retorna (ano, mes) para mensal, (ano, None) para anual, ou None se não identificado."""
    arq = nome_arquivo(url).lower()
    m = re.search(r"(?<!\d)(\d{2})_(\d{4})(?!\d)", arq)           # viagem_fretado_09_2026.csv
    if m:
        return int(m.group(2)), int(m.group(1))
    m = re.search(r"(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)(\d{4})", arq)  # ...-out2026.csv
    if m:
        return int(m.group(2)), MESES[m.group(1)]
    m = re.match(r"\s*(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)(\d{2})\s*-", (nome or "").lower())  # 'Set26 - ...'
    if m:
        return 2000 + int(m.group(2)), MESES[m.group(1)]
    m = re.search(r"(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)(\d{4})", (nome or "").lower())  # '... - Out2026'
    if m:
        return int(m.group(2)), MESES[m.group(1)]
    m = re.match(r"\s*(\d{4})\s*-", nome or "")                    # '2025 - Licenças ...'
    if m:
        return int(m.group(1)), None
    m = re.match(r"(\d{4})_", arq)                                 # 2025_licencas-...csv
    if m:
        return int(m.group(1)), None
    return None


def na_janela(per, tipo):
    if per is None:
        return False
    ano, mes = per
    if tipo == "anual" or mes is None:
        return INI[0] <= ano <= FIM[0]
    return INI <= (ano, mes) <= FIM


def baixar(url, destino):
    """Baixa em streaming; remove o arquivo parcial se falhar."""
    def _f():
        h = hashlib.sha256()
        total = 0
        try:
            with session.get(url, stream=True, timeout=(30, 120)) as r:
                r.raise_for_status()
                esperado = int(r.headers.get("Content-Length") or 0)
                enc = (r.headers.get("Content-Encoding") or "").lower()
                with open(destino, "wb") as f:
                    for bloco in r.iter_content(1024 * 1024):
                        f.write(bloco)
                        h.update(bloco)
                        total += len(bloco)
            if esperado and not enc and total != esperado:
                raise IOError(f"tamanho incompleto: {total} de {esperado} bytes")
        except Exception:
            if os.path.exists(destino):
                os.remove(destino)
            raise
        return total, h.hexdigest()
    return com_retentativa(_f, f"download {os.path.basename(destino)}")

# COMMAND ----------

# MAGIC %md ## Manifesto atual (último registro por fonte + url)

# COMMAND ----------

schema_manifesto = T.StructType([
    T.StructField("fonte", T.StringType()),
    T.StructField("recurso", T.StringType()),
    T.StructField("url", T.StringType()),
    T.StructField("last_modified", T.StringType()),
    T.StructField("bytes", T.LongType()),
    T.StructField("sha256", T.StringType()),
    T.StructField("baixado_em", T.TimestampType()),
    T.StructField("status", T.StringType()),
    T.StructField("mensagem", T.StringType()),
])

ultimo_ok = {}
for r in spark.table(MANIFESTO).where("status = 'ok'").orderBy("baixado_em").collect():
    # chave por nome do arquivo: o ANTT publica vários recursos com o mesmo nome de arquivo
    # (ex.: snapshots de empresas habilitadas no mesmo mês); vale o último gravado no Volume
    ultimo_ok[(r.fonte, nome_arquivo(r.url))] = r.last_modified

ja_ausentes = {(r.fonte, r.recurso) for r in spark.table(MANIFESTO).where("status = 'ausente_no_portal'").collect()}

print(f"{len(ultimo_ok)} arquivos já registrados com sucesso no manifesto; {len(ja_ausentes)} meses ausentes já registrados")

# COMMAND ----------

# MAGIC %md ## Consulta ao CKAN e download

# COMMAND ----------

pacotes = {}
ckan_erros = {}
for slug in sorted({FONTES[f]["slug"] for f in FONTES_ATIVAS}):
    try:
        pacotes[slug] = package_show(slug)
        print(f"[{slug}] {len(pacotes[slug].get('resources', []))} recursos")
    except Exception as e:
        ckan_erros[slug] = f"{type(e).__name__}: {e}"[:300]
        print(f"[{slug}] CKAN indisponível: {ckan_erros[slug]}")

resumo = {f: {"selecionados": 0, "baixados": 0, "pulados": 0, "erros": 0, "bytes_baixados": 0,
              "ckan": "ok", "arquivos_no_volume": 0} for f in FONTES_ATIVAS}
erros = []
registros = []
agora = datetime.now

for fonte in FONTES_ATIVAS:
    cfg = FONTES[fonte]
    pkg = pacotes.get(cfg["slug"])
    pasta = f"{VOL_PATH}/{fonte}"
    os.makedirs(pasta, exist_ok=True)
    if pkg is None:
        resumo[fonte]["ckan"] = "indisponivel"
        continue

    selecionados = []
    for res in pkg.get("resources", []):
        url = res.get("url") or ""
        nome = res.get("name") or ""
        if (res.get("format") or "").lower() != "csv" or not url.lower().endswith(".csv"):
            continue
        if "nome" in cfg and not re.search(cfg["nome"], nome, re.I):
            continue
        per = periodo_recurso(nome, url)
        if not na_janela(per, cfg["periodo"]):
            continue
        selecionados.append((per, res))
    # recursos repetidos para o mesmo arquivo (mesmo nome): fica o de last_modified mais recente
    por_arquivo = {}
    for per, res in selecionados:
        k = nome_arquivo(res["url"])
        lm_res = res.get("last_modified") or res.get("metadata_modified") or ""
        if k not in por_arquivo or lm_res > (por_arquivo[k][1].get("last_modified") or por_arquivo[k][1].get("metadata_modified") or ""):
            por_arquivo[k] = (per, res)
    resumo[fonte]["recursos_duplicados_descartados"] = len(selecionados) - len(por_arquivo)
    selecionados = sorted(por_arquivo.values(), key=lambda x: x[0])
    resumo[fonte]["selecionados"] = len(selecionados)

    # I2: meses esperados da janela sem recurso publicado entram no manifesto (status ausente_no_portal), uma vez só
    if cfg["periodo"] == "mensal":
        publicados = {f"{p[0]}-{p[1]:02d}" for p, _ in selecionados}
        a, m = INI
        ausentes = []
        while (a, m) <= FIM:
            rotulo = f"{a}-{m:02d}"
            if rotulo not in publicados and (fonte, rotulo) not in ja_ausentes:
                ausentes.append(rotulo)
            a, m = (a + 1, 1) if m == 12 else (a, m + 1)
        for rotulo in ausentes:
            registros.append((fonte, rotulo, None, None, None, None, agora(), "ausente_no_portal",
                              "mês esperado na janela sem recurso CSV no CKAN"))
        resumo[fonte]["ausentes_no_portal_novos"] = ausentes

    for per, res in selecionados:
        url, nome = res["url"], res.get("name")
        lm = res.get("last_modified") or res.get("metadata_modified")
        destino = f"{pasta}/{nome_arquivo(url)}"
        if ultimo_ok.get((fonte, nome_arquivo(url))) == lm and os.path.exists(destino):
            resumo[fonte]["pulados"] += 1
            print(f"[{fonte}] pulado (inalterado): {os.path.basename(destino)}")
            continue
        try:
            total, sha = baixar(url, destino)
            registros.append((fonte, nome, url, lm, total, sha, agora(), "ok", None))
            resumo[fonte]["baixados"] += 1
            resumo[fonte]["bytes_baixados"] += total
            print(f"[{fonte}] baixado: {os.path.basename(destino)} ({total:,} bytes)")
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"[:300]
            registros.append((fonte, nome, url, lm, None, None, agora(), "erro", msg))
            resumo[fonte]["erros"] += 1
            erros.append({"fonte": fonte, "recurso": nome, "url": url, "erro": msg})
            print(f"[{fonte}] ERRO: {nome}: {msg}")

if registros:
    spark.createDataFrame(registros, schema_manifesto).write.mode("append").saveAsTable(MANIFESTO)
    print(f"{len(registros)} registros gravados no manifesto")

# COMMAND ----------

# MAGIC %md ## Arquivos presentes no Volume (inclui fallback quando o CKAN falha)

# COMMAND ----------

for fonte in FONTES_ATIVAS:
    pasta = f"{VOL_PATH}/{fonte}"
    arquivos = [f for f in os.listdir(pasta) if f.lower().endswith(".csv")] if os.path.isdir(pasta) else []
    resumo[fonte]["arquivos_no_volume"] = len(arquivos)
    if resumo[fonte]["ckan"] != "ok":
        print(f"[{fonte}] CKAN indisponível, seguindo com {len(arquivos)} arquivos já no Volume")

saida = {
    "janela": [DATA_INICIO, DATA_FIM],
    "ckan_erros": ckan_erros,
    "fontes": resumo,
    "erros": erros,
}
print(json.dumps(saida, ensure_ascii=False, indent=2))
dbutils.notebook.exit(json.dumps(saida, ensure_ascii=False))
