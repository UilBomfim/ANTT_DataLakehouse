# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Silver: licenças (S6, S8, S2)
# MAGIC
# MAGIC - `silver.licenca_trecho`: SISAUT, 1 linha por (`licenca_id`, `sequencia_trecho`). Texto reparado (S2).
# MAGIC - `silver.licenca`: 1 linha por (`licenca_id`, `origem_registro`), `origem_registro` = `sisaut` | `continuo`.
# MAGIC - CPF de contratante mascarado (S8): nunca grava o CPF completo.
# MAGIC
# MAGIC **Decisão do contínuo:** 1 linha por `numero_autorizacao` com `qtd_placas` (contagem de placas distintas). A viagem liga
# MAGIC por autorização e não por placa; uma linha por placa repetiria origem/destino/passageiros N vezes e inflaria somas na Gold.
# MAGIC O contínuo é anual (arquivos 2025 e 2026): a mesma autorização pode aparecer nos dois. Fica a linha do arquivo mais recente.
# MAGIC
# MAGIC **Reparo de mojibake:** `value.encode('latin1').decode('utf-8')`, até 2 passadas. Caracteres que só existem em cp1252
# MAGIC (`ƒ`, `Š`...) voltam ao byte pelo cp1252, porque a dupla codificação passou por essa leitura. Aceita só se o resultado não tem mais
# MAGIC padrão de mojibake (`Ã`/`Â` seguido de caractere 0x80-0xFF, ou `U+FFFD`). Ver `silver_resultado.md` sobre a diferença
# MAGIC para a regra literal "sem Ã/Â" (um `Ã` legítimo como em "SÃO" é aceito).
# MAGIC
# MAGIC Escrita: overwrite determinístico das duas tabelas (derivadas por completo da Bronze, sem outra origem de escrita).
# MAGIC Reexecutar reproduz o mesmo resultado, sem duplicar.

# COMMAND ----------

import json
import re
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.text("data_inicio", "2025-10", "Data início (YYYY-MM)")
dbutils.widgets.text("data_fim", "2026-09", "Data fim (YYYY-MM)")
CATALOG = dbutils.widgets.get("catalog")
B = f"{CATALOG}.antt_bronze"
S = f"{CATALOG}.antt_silver"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {S}")
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

UFS = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI",
       "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]

COM_ACENTO = "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ"
SEM_ACENTO = "AAAAAEEEEIIIIOOOOOUUUUCN"


def norm_txt(c):
    """Maiúsculas, sem acento, trim; vazio vira nulo."""
    x = F.translate(F.upper(F.trim(c)), COM_ACENTO, SEM_ACENTO)
    return F.nullif(x, F.lit(""))


def norm_uf(c):
    return F.nullif(F.upper(F.trim(c)), F.lit(""))


def so_digitos(c):
    return F.regexp_replace(c, r"\D", "")


def cnpj14(c):
    d = so_digitos(c)
    return F.when(F.length(d).between(1, 14), F.lpad(d, 14, "0"))


def licenca_id(c):
    # só aceita valor puramente numérico: no SISAUT há ~30 linhas deslocadas (data no lugar do número da licença)
    t = F.trim(c)
    return F.when(t.rlike(r"^\d{1,18}$"), t.cast("bigint"))


def ts_multi(c, fmts):
    return F.coalesce(*[F.expr(f"try_to_timestamp(`{c}`, '{f}')") for f in fmts])


def int_seguro(c):
    return F.expr(f"try_cast(trim(`{c}`) as int)")


def dbl_ponto(c):
    return F.expr(f"try_cast(replace(trim(`{c}`), ',', '.') as double)")


# Mojibake: Ã/Â seguido de caractere 0x80-0xFF (latin1 lido de bytes UTF-8) ou U+FFFD
# Camadas intermediárias foram lidas como cp1252 (ex.: 'ƒ', 'Š', '‚'), então o segundo caractere pode estar fora de 0x80-0xFF.
_CP1252_EXTRA = "\u0152\u0153\u0160\u0161\u0178\u017d\u017e\u0192\u02c6\u02dc\u2013\u2014\u2018\u2019\u201a\u201c\u201d\u201e\u2020\u2021\u2022\u2026\u2030\u2039\u203a\u20ac\u2122"
_MOJI = re.compile("\ufffd|[ÃÂ][\u0080-\u00ff" + _CP1252_EXTRA + "]")


def _para_bytes(s):
    """Inverte a leitura errada: cada caractere volta ao byte original (cp1252 se existir, senão latin1)."""
    out = bytearray()
    for ch in s:
        try:
            out += ch.encode("cp1252")
        except UnicodeEncodeError:
            out += ch.encode("latin1")  # levanta UnicodeEncodeError se não couber em 1 byte
    return bytes(out)


def _reparar(v):
    """Retorna (texto, status): 'ok' (sem mojibake), 'corrigido' ou 'invalido' (mantém o original)."""
    if v is None:
        return (None, None)
    if not _MOJI.search(v):
        return (v, "ok")
    s = v
    for _ in range(2):
        try:
            s = _para_bytes(s).decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return (v, "invalido")
        if not _MOJI.search(s):
            # os campos da fonte são em caixa alta; a dupla codificação deixa ç/ã minúsculos no meio da palavra
            return (s.upper(), "corrigido")
    return (v, "invalido")


reparar_udf = F.udf(_reparar, T.StructType([T.StructField("t", T.StringType()), T.StructField("s", T.StringType())]))


def mask_doc(c):
    """S8: 11 dígitos (ou menos) = CPF -> '***.***.***-XX'; 14 (12-14) = CNPJ, mantido só em dígitos."""
    # A origem já mascara parte dos CPFs ('123.***.***-45', '1234*******'): contêm '*' e são tratados como CPF.
    # O resultado guarda só os 2 últimos dígitos; os 3 primeiros da origem são descartados.
    d = so_digitos(c)
    n = F.length(d)
    t = F.trim(c)
    fim2 = F.when(F.substring(t, -2, 2).rlike(r"^\d\d$"), F.substring(t, -2, 2)).otherwise(F.lit("**"))
    return (F.when(t.contains("*"), F.concat(F.lit("***.***.***-"), fim2))
            .when(n.between(1, 11), F.concat(F.lit("***.***.***-"), F.lpad(d, 11, "0").substr(10, 2)))
            .when(n.between(12, 14), F.lpad(d, 14, "0")))


def tipo_doc(c):
    n = F.length(so_digitos(c))
    return (F.when(F.trim(c).contains("*"), F.lit("CPF"))
            .when(n.between(1, 11), F.lit("CPF")).when(n.between(12, 14), F.lit("CNPJ")))


# COMMAND ----------
# MAGIC %md
# MAGIC ## SISAUT: trechos (com reparo de texto)

# COMMAND ----------

sis = spark.table(f"{B}.bronze_sisaut")
FMT_D = ["dd/MM/yyyy"]
FMT_DT = ["dd/MM/yyyy HH:mm:ss", "dd/MM/yyyy"]

t = (sis
     .withColumn("licenca_id", licenca_id(F.col("numero_licenca_viagem")))
     .withColumn("sequencia_trecho", int_seguro("sequencia_trecho"))
     .withColumn("cnpj", cnpj14(F.col("cnpj")))
     .withColumn("_ct", reparar_udf(F.col("contratante")))
     .withColumn("_li", reparar_udf(F.col("localidade_inicio_trecho")))
     .withColumn("_lf", reparar_udf(F.col("localidade_final_trecho"))))

trecho = (t.select(
    "licenca_id", "sequencia_trecho", "cnpj",
    F.col("tipo_trecho"),
    F.col("contratante").alias("contratante_original"),
    F.col("_ct.t").alias("contratante"), F.col("_ct.s").alias("contratante_texto"),
    norm_uf(F.col("uf_inicio_trecho")).alias("uf_inicio_trecho"),
    norm_txt(F.col("municipio_inicio_trecho")).alias("municipio_inicio_trecho"),
    F.col("localidade_inicio_trecho").alias("localidade_inicio_trecho_original"),
    F.col("_li.t").alias("localidade_inicio_trecho"), F.col("_li.s").alias("localidade_inicio_trecho_texto"),
    norm_uf(F.col("uf_final_trecho")).alias("uf_final_trecho"),
    norm_txt(F.col("municipio_final_trecho")).alias("municipio_final_trecho"),
    F.col("localidade_final_trecho").alias("localidade_final_trecho_original"),
    F.col("_lf.t").alias("localidade_final_trecho"), F.col("_lf.s").alias("localidade_final_trecho_texto"),
    ts_multi("data_hora_partida_inicio_trecho", FMT_DT).alias("data_hora_partida"),
    ts_multi("data_hora_chegada_final_trecho", FMT_DT).alias("data_hora_chegada"),
    dbl_ponto("distancia_trecho").alias("distancia_trecho"),
    "total_passageiros",
    F.col("_source_file").alias("arquivo_origem"), F.col("_file_mod_time"), F.col("_ingest_ts"), F.col("_row_hash"),
).where(F.col("licenca_id").isNotNull() & F.col("sequencia_trecho").isNotNull()))

# dedup: chave licenca_id+sequencia_trecho; fica o registro de arquivo mais recente (a licença pode reaparecer em outro mês)
w = Window.partitionBy("licenca_id", "sequencia_trecho").orderBy(F.col("_file_mod_time").desc(), F.col("_ingest_ts").desc(), F.col("_row_hash"))
trecho = (trecho.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn", "_file_mod_time", "_ingest_ts", "_row_hash")
          .withColumn("uf_inicio_valida", F.col("uf_inicio_trecho").isin(UFS))
          .withColumn("uf_final_valida", F.col("uf_final_trecho").isin(UFS)))
(trecho.drop("total_passageiros").write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{S}.licenca_trecho"))
spark.sql(f"COMMENT ON TABLE {S}.licenca_trecho IS 'SISAUT: 1 linha por (licenca_id, sequencia_trecho); texto reparado, *_original preserva o valor da Bronze'")

# COMMAND ----------
# MAGIC %md
# MAGIC ## SISAUT: licença (agregada por numero_licenca_viagem)

# COMMAND ----------

# colunas de nível licença vêm da linha mais recente do arquivo; trechos já estão em licenca_trecho
base = (sis
        .withColumn("licenca_id", licenca_id(F.col("numero_licenca_viagem")))
        .withColumn("sequencia_trecho", int_seguro("sequencia_trecho"))
        .where(F.col("licenca_id").isNotNull() & F.col("sequencia_trecho").isNotNull())
        .withColumn("pax", int_seguro("total_passageiros"))
        .withColumn("_ini", ts_multi("data_inicio_viagem", FMT_D).cast("date"))
        .withColumn("_fim", ts_multi("data_fim_viagem", FMT_D).cast("date")))

# total_passageiros constante dentro da licença?
pax_stats = base.groupBy("licenca_id").agg(F.countDistinct("pax").alias("n_pax"), F.collect_set("pax").alias("pax_set"))
pax_nao_const = pax_stats.where("n_pax > 1").count()
pax_total_lic = pax_stats.count()
# regra para licença não constante: maior valor (a licença é para o veículo cheio); a quantidade fica reportada
pax_pick = pax_stats.select("licenca_id", F.array_max("pax_set").alias("total_passageiros"), (F.col("n_pax") > 1).alias("pax_nao_constante"))

tr = spark.table(f"{S}.licenca_trecho")
ult = (tr.groupBy("licenca_id").agg(
    F.max_by(F.struct("municipio_final_trecho", "uf_final_trecho"), F.col("sequencia_trecho")).alias("destino"),
    # candidatos a destino principal: fim de cada trecho com a distância do trecho (struct ordena por distancia primeiro)
    F.collect_list(F.struct(F.col("distancia_trecho").alias("distancia"), F.col("municipio_final_trecho").alias("municipio"),
                            F.col("uf_final_trecho").alias("uf"))).alias("_finais"),
    F.min_by("contratante", F.col("sequencia_trecho")).alias("contratante"),
    F.min_by("contratante_original", F.col("sequencia_trecho")).alias("contratante_original"),
    F.min_by("contratante_texto", F.col("sequencia_trecho")).alias("contratante_texto"),
    F.count("*").alias("qtd_trechos"),
    F.sum("distancia_trecho").alias("soma_distancia_trechos")))

wl = Window.partitionBy("licenca_id").orderBy(F.col("_file_mod_time").desc(), F.col("_ingest_ts").desc(), F.col("sequencia_trecho").asc(), F.col("_row_hash"))
head = (base.withColumn("_rn", F.row_number().over(wl)).where("_rn = 1").select(
    "licenca_id",
    cnpj14(F.col("cnpj")).alias("cnpj"),
    F.trim(F.col("razao_social")).alias("razao_social"),
    mask_doc(F.col("cnpj_cpf_contratante")).alias("cnpj_cpf_contratante"),
    tipo_doc(F.col("cnpj_cpf_contratante")).alias("tipo_doc_contratante"),
    norm_txt(F.col("municipio_origem")).alias("municipio_origem"),
    norm_uf(F.col("uf_municipio_origem")).alias("uf_origem"),
    F.trim(F.col("tipo_viagem")).alias("tipo_viagem"),
    dbl_ponto("distancia_total").alias("distancia_total"),
    F.col("_source_file").alias("arquivo_origem")))
per = base.groupBy("licenca_id").agg(F.min("_ini").alias("data_inicio"), F.max("_fim").alias("data_fim"))

# Destino principal: o fim do trecho MAIS LONGO cujo município/UF difere da origem da licença. O fretamento é em circuito fechado,
# então o último trecho costuma voltar à origem e não representa o destino. Se todos os trechos terminam na origem (viagem dentro
# de um município), o destino principal é a própria origem. O destino do último trecho fica preservado em *_ultimo_trecho.
_dif = F.filter("_finais", lambda x: x["municipio"].isNotNull()
                & ~((x["municipio"] == F.col("municipio_origem")) & x["uf"].eqNullSafe(F.col("uf_origem"))))
_dp = F.array_max(_dif)  # struct ordena por distancia (nulos por último); null se _dif vazio

lic_sis = (head.join(per, "licenca_id").join(pax_pick, "licenca_id").join(ult, "licenca_id")
           .select(
               "licenca_id", F.lit("sisaut").alias("origem_registro"),
               "cnpj", "razao_social", "cnpj_cpf_contratante", "tipo_doc_contratante",
               "contratante", "contratante_original", "contratante_texto",
               "municipio_origem", "uf_origem",
               F.coalesce(_dp["municipio"], F.col("municipio_origem")).alias("municipio_destino"),
               F.coalesce(_dp["uf"], F.col("uf_origem")).alias("uf_destino"),
               F.col("destino.municipio_final_trecho").alias("municipio_destino_ultimo_trecho"),
               F.col("destino.uf_final_trecho").alias("uf_destino_ultimo_trecho"),
               "total_passageiros", "pax_nao_constante", "data_inicio", "data_fim", "tipo_viagem", "distancia_total",
               F.lit(None).cast("int").alias("qtd_placas"),
               "arquivo_origem", "qtd_trechos"))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Contínuo: 1 linha por numero_autorizacao

# COMMAND ----------

c = spark.table(f"{B}.bronze_licencas_continuo")
c = (c.withColumn("licenca_id", licenca_id(F.col("numero_autorizacao")))
     .where(F.col("licenca_id").isNotNull())
     .withColumn("_ini", ts_multi("data_inicio_vigencia", FMT_D).cast("date"))
     .withColumn("_fim", ts_multi("data_fim_vigencia", FMT_D).cast("date")))
wc = Window.partitionBy("licenca_id").orderBy(F.col("_ini").desc_nulls_last(), F.col("_file_mod_time").desc(), F.col("_ingest_ts").desc(), F.col("placa"))
placas = c.groupBy("licenca_id").agg(F.countDistinct(F.upper(F.trim("placa"))).cast("int").alias("qtd_placas"))
ch = c.withColumn("_rn", F.row_number().over(wc)).where("_rn = 1")
lic_cont = (ch.join(placas, "licenca_id").select(
    "licenca_id", F.lit("continuo").alias("origem_registro"),
    cnpj14(F.col("cnpj")).alias("cnpj"),
    F.trim(F.col("razao_social")).alias("razao_social"),
    mask_doc(F.col("cnpj_contratante")).alias("cnpj_cpf_contratante"),
    tipo_doc(F.col("cnpj_contratante")).alias("tipo_doc_contratante"),
    F.trim(F.col("razao_social_contratante")).alias("contratante"),
    F.trim(F.col("razao_social_contratante")).alias("contratante_original"),
    F.lit("ok").alias("contratante_texto"),
    norm_txt(F.col("municipio_origem")).alias("municipio_origem"),
    norm_uf(F.col("uf_municipio_origem")).alias("uf_origem"),
    norm_txt(F.col("municipio_destino")).alias("municipio_destino"),
    norm_uf(F.col("uf_municipio_destino")).alias("uf_destino"),
    F.lit(None).cast("string").alias("municipio_destino_ultimo_trecho"),
    F.lit(None).cast("string").alias("uf_destino_ultimo_trecho"),
    int_seguro("total_passageiros").alias("total_passageiros"),
    F.lit(False).alias("pax_nao_constante"),
    F.col("_ini").alias("data_inicio"), F.col("_fim").alias("data_fim"),
    F.lit(None).cast("string").alias("tipo_viagem"),
    F.lit(None).cast("double").alias("distancia_total"),
    placas["qtd_placas"],
    F.col("_source_file").alias("arquivo_origem"),
    F.lit(None).cast("long").alias("qtd_trechos")))

# COMMAND ----------

cols = ["licenca_id", "origem_registro", "cnpj", "razao_social", "cnpj_cpf_contratante", "tipo_doc_contratante",
        "contratante", "contratante_original", "contratante_texto", "municipio_origem", "uf_origem", "municipio_destino",
        "uf_destino", "municipio_destino_ultimo_trecho", "uf_destino_ultimo_trecho", "total_passageiros", "pax_nao_constante",
        "data_inicio", "data_fim", "tipo_viagem", "distancia_total", "qtd_placas", "qtd_trechos", "arquivo_origem"]
lic = (lic_sis.select(*cols).unionByName(lic_cont.select(*cols))
       .withColumn("uf_valida", F.col("uf_origem").isin(UFS) & F.col("uf_destino").isin(UFS))
       .withColumn("cnpj_valido_formato", F.col("cnpj").isNotNull()))
(lic.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.licenca"))
spark.sql(f"COMMENT ON TABLE {S}.licenca IS 'Licenças: 1 linha por (licenca_id, origem_registro sisaut|continuo). CPF de contratante mascarado'")

# COMMAND ----------

L = spark.table(f"{S}.licenca")
TR = spark.table(f"{S}.licenca_trecho")
cpf_completo = L.where(F.col("cnpj_cpf_contratante").rlike(r"^\d{11}$") | F.col("cnpj_cpf_contratante").rlike(r"\d{3}\.\d{3}\.\d{3}-\d{2}")).count()
txt = {}
for col in ["contratante", "localidade_inicio_trecho", "localidade_final_trecho"]:
    orig = "contratante_original" if col == "contratante" else f"{col}_original"
    st = {str(r[0]): r[1] for r in TR.groupBy(f"{col}_texto").count().collect()}
    aceitos = TR.where(F.col(f"{col}_texto") == "corrigido")
    txt[col] = {
        "status_linhas_trecho": st,
        "valores_distintos_corrigidos": aceitos.select(orig).distinct().count(),
        "valores_distintos_invalidos": TR.where(F.col(f"{col}_texto") == "invalido").select(orig).distinct().count(),
        "aceitos_com_Ã_Â_ou_FFFD_literal": aceitos.where(F.col(col).contains("Ã") | F.col(col).contains("Â") | F.col(col).contains("\ufffd")).count(),
        "aceitos_com_Ã_ou_Â_seguido_de_nao_letra_ascii": aceitos.where(F.col(col).rlike("[ÃÂ][^A-Z]")).count(),
        "aceitos_com_padrao_mojibake": aceitos.where(F.col(col).rlike("\ufffd|[ÃÂ][\u0080-\u00ff" + _CP1252_EXTRA + "]")).count(),
        "exemplos": [[r[0], r[1]] for r in aceitos.select(orig, col).distinct().limit(6).collect()],
        "exemplos_invalidos": [r[0] for r in TR.where(F.col(f"{col}_texto") == "invalido").select(orig).distinct().limit(6).collect()],
    }
nulos_data = {
    "sisaut_sem_data_inicio": L.where("origem_registro='sisaut' and data_inicio is null").count(),
    "sisaut_sem_data_fim": L.where("origem_registro='sisaut' and data_fim is null").count(),
    "continuo_sem_data_inicio": L.where("origem_registro='continuo' and data_inicio is null").count(),
    "continuo_sem_data_fim": L.where("origem_registro='continuo' and data_fim is null").count(),
    "trecho_sem_partida": TR.where("data_hora_partida is null").count(),
}
r = {
    "bronze_sisaut": sis.count(), "sisaut_linhas_malformadas_descartadas": sis.count() - base.count(), "bronze_continuo": spark.table(f"{B}.bronze_licencas_continuo").count(),
    "licenca": L.count(),
    "licenca_por_origem": {x[0]: x[1] for x in L.groupBy("origem_registro").count().collect()},
    "licenca_trecho": TR.count(),
    "duplicatas_chave_licenca": L.groupBy("licenca_id", "origem_registro").count().where("count > 1").count(),
    "duplicatas_chave_trecho": TR.groupBy("licenca_id", "sequencia_trecho").count().where("count > 1").count(),
    "licenca_id_em_ambas_origens": L.groupBy("licenca_id").count().where("count > 1").count(),
    "sisaut_licencas": pax_total_lic, "sisaut_licencas_pax_nao_constante": pax_nao_const,
    "cpf_completo_em_licenca": cpf_completo,
    "tipo_doc": {str(x[0]): x[1] for x in L.groupBy("tipo_doc_contratante").count().collect()},
    "cpf_mascarado_exemplos": [x[0] for x in L.where("tipo_doc_contratante = 'CPF'").select("cnpj_cpf_contratante").limit(3).collect()],
    "uf_invalida": L.where(~F.col("uf_valida") | F.col("uf_valida").isNull()).count(),
    "cnpj_nulo": L.where("cnpj is null").count(),
    "pax_zero_ou_nulo": L.where("total_passageiros is null or total_passageiros <= 0").count(),
    "continuo_qtd_placas_max": L.where("origem_registro='continuo'").agg(F.max("qtd_placas")).collect()[0][0],
    "mojibake": txt,
    "datas_nulas": nulos_data,
}
print(json.dumps(r, ensure_ascii=False, indent=1, default=str))
dbutils.notebook.exit(json.dumps(r, ensure_ascii=False, default=str))
