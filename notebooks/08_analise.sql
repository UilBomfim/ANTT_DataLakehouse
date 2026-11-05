-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 08 - Análise: respostas às 6 perguntas do roteiro
-- MAGIC Consultas sobre `workspace.antt_gold`. Cada seção responde a uma pergunta de negócio e começa com a explicação das colunas usadas.
-- MAGIC
-- MAGIC **Janela de dados:** out/2025 a set/2026 (12 meses).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Glossário (vale para todo o notebook)
-- MAGIC
-- MAGIC | Termo | Significado |
-- MAGIC |---|---|
-- MAGIC | **Monitriip** | Sistema da ANTT que registra as viagens realizadas por rastreamento. Fonte de *viagens realizadas*. |
-- MAGIC | **Licença / SISAUT** | Autorização prévia da viagem de fretamento (eventual e turístico = SISAUT; fretamento contínuo = outra base). Fonte de *viagens licenciadas*. Uma licença tem origem, destino e número de passageiros declarados. |
-- MAGIC | **Viagem realizada x licenciada** | Realizada = registrada pelo Monitriip. Licenciada = autorizada. São **medidas diferentes**: não some uma com a outra. |
-- MAGIC | **Fretado x regular** | Fretado = grupo fechado, sem venda individual. Regular = linhas com venda de bilhete (fonte: bilhetes de passagem). |
-- MAGIC | **Bilhete** | A base é *agregada*: cada linha já traz a quantidade de bilhetes de um mesmo mês, origem, destino, tipo de serviço e gratuidade. Não há CNPJ, por isso bilhetes não entram no ranking de empresas. |
-- MAGIC | **Passageiros** | Número *declarado na licença*, contado uma vez por licença. Não é o embarque efetivo. |
-- MAGIC | **Cobertura (`status_cobertura`)** | `ok` = mês publicado e completo; `ausente` = a ANTT não publicou o mês; `parcial` = publicado, mas com menos de 25% do volume típico. |
-- MAGIC | **Nulo (vazio) x zero** | Mês `ausente` aparece **vazio**, nunca zero. Zero significaria que não houve viagens, e não é o caso: o dado não existe. |
-- MAGIC | **`localidade_key`** | Chave do local no formato `MUNICIPIO|UF`, em maiúsculas e sem acento (ex.: `SAO PAULO|SP`). |
-- MAGIC | **MoM / YoY** | Variação percentual contra o mês anterior (MoM) ou o mesmo mês do ano anterior (YoY). Só é calculada quando os dois meses são completos. YoY fica vazio em toda a janela, pois ela tem só 12 meses. |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Quantas viagens foram realizadas por período?
-- MAGIC
-- MAGIC **Tabela:** `agg_viagens_periodo` (uma linha por nível e período).
-- MAGIC
-- MAGIC | Coluna | O que é |
-- MAGIC |---|---|
-- MAGIC | `nivel` | Granularidade: `mes`, `semana` ou `dia`. |
-- MAGIC | `periodo` | Mês (`2026-03`), semana ISO (`2026-S14`) ou dia (`2026-03-15`), conforme o nível. A viagem é contada pela **data de início**. |
-- MAGIC | `viagens_realizadas` | Quantidade de viagens registradas pelo Monitriip. **Vazio** em mês sem publicação (fev/2026). |
-- MAGIC | `viagens_licenciadas` | Quantidade de licenças (SISAUT + fretamento contínuo) com início no período. Cobre fev/2026, que não tem dado do Monitriip. |
-- MAGIC | `status_cobertura` | Situação do mês na base de viagens (`ok`, `ausente`, `parcial`). |
-- MAGIC | `viagens_realizadas_mom_pct` | Variação % de viagens realizadas contra o mês anterior. Vazia se um dos dois meses não for completo (por isso março/2026 fica vazio). |

-- COMMAND ----------

-- 1a. Viagens por mês (realizadas x licenciadas)
SELECT periodo AS mes, viagens_realizadas, viagens_licenciadas, status_cobertura, viagens_realizadas_mom_pct
FROM workspace.antt_gold.agg_viagens_periodo
WHERE nivel = 'mes'
ORDER BY periodo

-- COMMAND ----------

-- 1b. Dez dias com mais viagens realizadas
SELECT periodo AS dia, viagens_realizadas
FROM workspace.antt_gold.agg_viagens_periodo
WHERE nivel = 'dia'
ORDER BY viagens_realizadas DESC, periodo
LIMIT 10

-- COMMAND ----------

-- 1c. Viagens por semana (semanas ISO)
SELECT periodo AS semana, viagens_realizadas, status_cobertura
FROM workspace.antt_gold.agg_viagens_periodo
WHERE nivel = 'semana'
ORDER BY periodo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Quais empresas possuem maior volume de viagens?
-- MAGIC
-- MAGIC **Tabela:** `agg_ranking_empresas` (uma linha por empresa e período).
-- MAGIC
-- MAGIC | Coluna | O que é |
-- MAGIC |---|---|
-- MAGIC | `periodo_tipo` | `total` = soma da janela inteira; `mes` = ranking de cada mês. |
-- MAGIC | `periodo` | `total` ou o mês (`2026-03`). |
-- MAGIC | `cnpj` | CNPJ da empresa, 14 dígitos, sem máscara. |
-- MAGIC | `razao_social` | Nome da empresa, vindo do cadastro de empresas habilitadas. `NAO_HABILITADA` = o CNPJ fez viagens mas não consta no cadastro. |
-- MAGIC | `viagens` | Viagens realizadas (Monitriip) da empresa. |
-- MAGIC | `passageiros` | Passageiros **declarados nas licenças** das viagens da empresa. Cada licença conta uma vez (no mês da primeira viagem), para não duplicar. |
-- MAGIC | `rank_viagens` | Posição por número de viagens (1 = maior). Em empate, desempata pelo CNPJ. |
-- MAGIC
-- MAGIC Observação: o ranking considera só o **fretado**. Os bilhetes não têm CNPJ.

-- COMMAND ----------

-- 2a. Top 10 empresas na janela inteira (passageiros contados uma vez por licença)
SELECT rank_viagens AS posicao, razao_social, cnpj, viagens, passageiros
FROM workspace.antt_gold.agg_ranking_empresas
WHERE periodo_tipo = 'total' AND rank_viagens <= 10
ORDER BY rank_viagens

-- COMMAND ----------

-- 2b. Três maiores empresas de cada mês
SELECT periodo AS mes, rank_viagens AS posicao, razao_social, viagens
FROM workspace.antt_gold.agg_ranking_empresas
WHERE periodo_tipo = 'mes' AND rank_viagens <= 3
ORDER BY periodo, rank_viagens

-- COMMAND ----------

-- 2c. Viagens de empresas que não constam como habilitadas na dimensão de empresas
SELECT periodo_tipo, periodo, razao_social, viagens
FROM workspace.antt_gold.agg_ranking_empresas
WHERE razao_social = 'NAO_HABILITADA' AND periodo_tipo = 'total'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Quais são as principais origens e destinos?
-- MAGIC
-- MAGIC **Tabela:** `agg_origem_destino` (top 200 por visão e tipo).
-- MAGIC
-- MAGIC | Coluna | O que é |
-- MAGIC |---|---|
-- MAGIC | `visao` | `fretado` = viagens fretadas (Monitriip + licença). `regular` = linhas regulares (bilhetes vendidos). |
-- MAGIC | `tipo` | `origem`, `destino` ou `par` (origem → destino). |
-- MAGIC | `localidade_key` | Local (`MUNICIPIO|UF`) ou, no tipo `par`, `ORIGEM -> DESTINO`. |
-- MAGIC | `municipio`, `uf` | Município e estado do local. Vazios no tipo `par`. |
-- MAGIC | `viagens` | **Fretado:** viagens realizadas com aquele local. Vazio na visão regular. |
-- MAGIC | `passageiros` | **Fretado:** passageiros declarados nas licenças com aquele local (base de licenças, não de viagens, por isso não é proporcional a `viagens`). **Regular:** quantidade de **bilhetes** vendidos (mês da viagem, só meses confiáveis). |
-- MAGIC | `rank` | Posição dentro da visão e do tipo (1 = maior volume). |
-- MAGIC
-- MAGIC **Como o destino do fretado é definido:** o fretamento costuma ser em circuito fechado (sai e volta ao mesmo lugar). Por isso o destino da licença é o **fim do trecho mais longo cujo local seja diferente da origem**. Se todos os trechos terminam na origem, o destino é a própria origem. É uma aproximação.

-- COMMAND ----------

-- 3a. Fretado: principais destinos (viagens realizadas, destino principal da licença)
SELECT rank AS posicao, municipio, uf, viagens, passageiros
FROM workspace.antt_gold.agg_origem_destino
WHERE visao = 'fretado' AND tipo = 'destino'
ORDER BY rank
LIMIT 10

-- COMMAND ----------

-- 3b. Fretado: principais origens
SELECT rank AS posicao, municipio, uf, viagens, passageiros
FROM workspace.antt_gold.agg_origem_destino
WHERE visao = 'fretado' AND tipo = 'origem'
ORDER BY rank
LIMIT 10

-- COMMAND ----------

-- 3c. Fretado: principais pares origem -> destino
SELECT rank AS posicao, localidade_key AS par_origem_destino, viagens
FROM workspace.antt_gold.agg_origem_destino
WHERE visao = 'fretado' AND tipo = 'par'
ORDER BY rank
LIMIT 10

-- COMMAND ----------

-- 3d. Regular: principais pares (bilhetes por mês da viagem, só meses confiáveis)
SELECT rank AS posicao, localidade_key AS par_origem_destino, passageiros AS bilhetes
FROM workspace.antt_gold.agg_origem_destino
WHERE visao = 'regular' AND tipo = 'par'
ORDER BY rank
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Qual é a duração média das viagens?
-- MAGIC
-- MAGIC **Tabela:** `agg_duracao_media` (uma linha por nível e período).
-- MAGIC
-- MAGIC | Coluna | O que é |
-- MAGIC |---|---|
-- MAGIC | `nivel` | `mes` = todas as empresas no mês; `empresa_mes` = por empresa e mês (coluna `cnpj` preenchida). |
-- MAGIC | `periodo` | Mês da **data de início** da viagem. |
-- MAGIC | `viagens_consideradas` | Viagens usadas no cálculo. É menor que o total de viagens do mês, porque exclui as descritas abaixo. |
-- MAGIC | `duracao_media_min` | Média da duração, em minutos (fim menos início da viagem). |
-- MAGIC | `duracao_mediana_min` | Mediana: metade das viagens dura menos que isso. |
-- MAGIC | `duracao_p90_min` | Percentil 90 (aproximado): 90% das viagens duram menos que isso. |
-- MAGIC | `status_cobertura` | Situação do mês; mês `ausente` vem vazio. |
-- MAGIC
-- MAGIC **O que fica de fora:** viagens com duração atípica (menos de 1 minuto ou mais de 30 dias) e a cópia mantida dos códigos de viagem repetidos entre arquivos.
-- MAGIC
-- MAGIC **Como ler:** a média (cerca de 37 horas) é bem maior que a mediana (cerca de 13 horas), porque poucas viagens muito longas puxam a média. Para o dia a dia, a **mediana** representa melhor.

-- COMMAND ----------

-- 4a. Duração por mês, em minutos e em horas (exclui duração atípica e códigos repetidos)
SELECT periodo AS mes, viagens_consideradas,
       duracao_media_min, ROUND(duracao_media_min / 60, 1) AS duracao_media_horas,
       duracao_mediana_min, ROUND(duracao_mediana_min / 60, 1) AS duracao_mediana_horas,
       duracao_p90_min, status_cobertura
FROM workspace.antt_gold.agg_duracao_media
WHERE nivel = 'mes'
ORDER BY periodo

-- COMMAND ----------

-- 4b. Média da janela inteira, ponderada pelo número de viagens (meses completos)
SELECT SUM(viagens_consideradas) AS viagens,
       ROUND(SUM(duracao_media_min * viagens_consideradas) / SUM(viagens_consideradas) / 60, 1) AS duracao_media_horas
FROM workspace.antt_gold.agg_duracao_media
WHERE nivel = 'mes' AND mes_completo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Existem registros duplicados ou inconsistentes?
-- MAGIC
-- MAGIC **Tabela `agg_qualidade_registros`** (uma linha por regra, e por regra e mês para os alertas das viagens).
-- MAGIC
-- MAGIC | Coluna | O que é |
-- MAGIC |---|---|
-- MAGIC | `origem` | De onde vem a linha: `regra_dq` (regras de qualidade da Silver), `quarentena` (registros descartados), `cobertura` (meses ausentes/parciais) ou `flag_por_mes` (alertas das viagens, por mês). |
-- MAGIC | `tabela` | Tabela avaliada (viagem, licença, parada, bilhete...). |
-- MAGIC | `regra` | Nome da regra. Exemplos: `duracao_atipica` (viagem com menos de 1 min ou mais de 30 dias), `sem_licenca` (viagem sem licença correspondente), `empresa_nao_habilitada` (CNPJ fora do cadastro), `codigo_repetido_entre_arquivos` (mesmo código de viagem em dois arquivos), `uf_invalida`, `mes_viagem_antes_emissao`. |
-- MAGIC | `severidade` | `critica` = o registro é descartado ou a chave é violada; `alerta` = o registro fica, sinalizado; `info` = observação. |
-- MAGIC | `avaliados` | Quantos registros a regra examinou. |
-- MAGIC | `violados` | Quantos descumpriram a regra. |
-- MAGIC | `pct_violado` | `violados / avaliados`, em %. |
-- MAGIC | `detalhe` | Informação extra: o mês (nas linhas `flag_por_mes`) ou a lista de fontes e meses (nas linhas de cobertura). |
-- MAGIC
-- MAGIC **Tabela `cobertura_fonte_mes`** (uma linha por fonte e mês): `fonte`, `ano_mes`, `status` (`ok`/`ausente`/`parcial`), `linhas` (registros carregados no mês), `pct_mediana` (volume do mês em relação à mediana dos outros meses da mesma fonte) e `observacao` (explicação).
-- MAGIC
-- MAGIC **Tabela `fato_bilhete`** (consulta 6b): `status_mes_viagem` indica se o mês da viagem é confiável para medir volume transportado (`incompleto_inicio`, `confiavel` ou `agendado`; ver a seção 6).
-- MAGIC
-- MAGIC ### O que acontece com os registros que violam uma regra
-- MAGIC
-- MAGIC A gravidade da regra (`severidade`) define o tratamento geral:
-- MAGIC
-- MAGIC | Severidade | Tratamento |
-- MAGIC |---|---|
-- MAGIC | `critica` | O registro é **retirado da tabela principal e vai para uma tabela de quarentena** (com o motivo), ou a chave da tabela seria violada. Fica fora da Gold. |
-- MAGIC | `alerta` | O registro **fica** na tabela, **sinalizado** (coluna de flags), e segue para a Gold. Nada é apagado nem corrigido. |
-- MAGIC | `info` | Só observação (cobertura, plausibilidade). O registro não é alterado. |
-- MAGIC
-- MAGIC Tratamento aplicado a cada regra que teve violações nesta carga:
-- MAGIC
-- MAGIC | Regra | Tabela | O que acontece com o registro | Efeito nos indicadores |
-- MAGIC |---|---|---|---|
-- MAGIC | `codigo_repetido_entre_arquivos` | viagem | O mesmo código aparece em dois arquivos (são duas viagens diferentes). Fica a cópia **mais recente**, sinalizada; a outra vai para a **quarentena**. | A cópia mantida entra nas contagens, mas fica fora da duração média. A cópia em quarentena não entra. |
-- MAGIC | `duracao_atipica` | viagem | Mantida e sinalizada (menos de 1 minuto ou mais de 30 dias). | Fora da duração média; entra nas contagens de viagens. |
-- MAGIC | `sem_licenca` | viagem | Mantida e sinalizada (nenhuma licença com o mesmo número). | Entra nas contagens e no ranking de empresas; fica **sem origem e destino**. |
-- MAGIC | `empresa_nao_habilitada` | viagem | Mantida e sinalizada (CNPJ fora do cadastro de habilitadas). | Aparece como `NAO_HABILITADA` no ranking de empresas. |
-- MAGIC | `mes_parcial` | parada | Mantida e sinalizada (arquivo de paradas incompleto, ago/2026). | Não entra na contagem de paradas por viagem. |
-- MAGIC | `data_parada_em_mes_diferente_do_arquivo` | parada | Mantida. A parada pode ocorrer no mês seguinte ao da viagem. | O mês da parada vem da data da parada, não do arquivo. |
-- MAGIC | `mes_viagem_antes_emissao` | bilhete | Mantida e sinalizada (viagem anterior à venda; provável erro de data na origem). | Entra na série pelo mês da viagem informado. |
-- MAGIC | `uf_invalida` | bilhete, licença | Mantida e sinalizada (UF fora das 27, por exemplo vazia). | Sem UF o local não é identificado nos rankings de origem e destino. |
-- MAGIC | `uf_inicio_invalida`, `uf_final_invalida` | trecho de licença | Mantida e sinalizada. | Pode deixar o destino da licença sem local identificado. |
-- MAGIC | `total_passageiros_fora_de_1_a_99` | licença | Mantida e sinalizada. | Os passageiros são somados **como declarados**; vale revisar os casos extremos. |
-- MAGIC | `total_passageiros_nao_constante_na_licenca` | licença | Mantida; adota-se o **maior valor** declarado entre os trechos. | Passageiros da licença = maior valor. |
-- MAGIC | `data_fim_antes_data_inicio` | licença | Mantida e sinalizada. | Sem efeito nos indicadores atuais (usam a data de início). |
-- MAGIC | `contratante_*_nao_reparado`, `localidade_*_nao_reparada` | licença, trecho | O texto vinha com acentos corrompidos. A Silver tenta consertar; o que não conserta **fica com o texto original**. | Sem efeito: nome do contratante e localidade em texto livre não entram nos indicadores. |
-- MAGIC | `texto_reparado_*` (informativo) | trecho | Texto corrigido automaticamente; o original fica guardado em outra coluna. | Nenhum. |
-- MAGIC | `mes_ausente`, `mes_parcial` | cobertura | Só informação: o mês não foi publicado ou veio incompleto. | O indicador do mês aparece **vazio** (nunca zero) e variações que o usam também. |
-- MAGIC
-- MAGIC **Duplicados removidos na limpeza** (não aparecem como violação, porque já foram tratados): linhas idênticas e repetidas de paradas (4.326) e paradas com a mesma chave (2.758), mantendo a mais recente; trechos de licença repetidos entre arquivos (125), mantendo o mais recente; 30 linhas do SISAUT com colunas deslocadas, sem número de licença válido, que foram descartadas.
-- MAGIC
-- MAGIC **Limite atual:** só `viagem_fretado` e `parada_fretado` têm tabela de quarentena. Nas outras tabelas (empresa, licença, bilhete), as regras `critica` (por exemplo `cnpj_nulo`, `quantidade_nula_ou_nao_positiva`, `cpf_completo_exposto`) apenas **medem**. Nesta carga todas tiveram **0 ocorrências**, então não houve registro afetado. Se ocorrerem, hoje o registro seguiria na Silver; criar quarentena para essas tabelas é um ajuste previsto para produção.

-- COMMAND ----------

-- 5a. Regras de qualidade com violações (da maior para a menor), com o tratamento dado ao registro
SELECT tabela, regra, severidade, avaliados, violados, ROUND(pct_violado, 3) AS pct_violado,
  CASE
    WHEN regra = 'motivo_codigo_repetido_entre_arquivos' THEN 'Copia descartada: vai para a quarentena e fica fora da Gold'
    WHEN regra LIKE 'motivo_%' OR regra = 'chave_natural_nula_ou_invalida' THEN 'Vai para a quarentena e fica fora da Gold'
    WHEN regra = 'codigo_repetido_entre_arquivos' THEN 'Copia mais recente mantida e sinalizada; fora da duracao media'
    WHEN regra = 'duracao_atipica' THEN 'Mantida e sinalizada; fora da duracao media'
    WHEN regra = 'sem_licenca' THEN 'Mantida e sinalizada; sem origem e destino'
    WHEN regra = 'empresa_nao_habilitada' THEN 'Mantida e sinalizada; aparece como NAO_HABILITADA'
    WHEN regra = 'mes_parcial' AND tabela = 'parada_fretado' THEN 'Mantida e sinalizada; nao conta nas paradas da Gold'
    WHEN regra = 'data_parada_em_mes_diferente_do_arquivo' THEN 'Mantida; mes pela data da parada'
    WHEN regra = 'mes_viagem_antes_emissao' THEN 'Mantida e sinalizada; entra pelo mes da viagem informado'
    WHEN regra IN ('uf_invalida', 'uf_inicio_invalida', 'uf_final_invalida') THEN 'Mantida e sinalizada; sem UF o local nao entra nos rankings'
    WHEN regra = 'total_passageiros_fora_de_1_a_99' THEN 'Mantida e sinalizada; somada como declarada'
    WHEN regra = 'total_passageiros_nao_constante_na_licenca' THEN 'Mantida; usa o maior valor da licenca'
    WHEN regra = 'data_fim_antes_data_inicio' THEN 'Mantida e sinalizada'
    WHEN regra LIKE '%nao_reparad%' THEN 'Mantida com o texto original; nao afeta indicadores'
    ELSE 'Ver o quadro de tratamento acima'
  END AS tratamento
FROM workspace.antt_gold.agg_qualidade_registros
WHERE origem IN ('regra_dq', 'quarentena') AND violados > 0
ORDER BY violados DESC
LIMIT 25

-- COMMAND ----------

-- 5b. Meses ausentes ou parciais por fonte
SELECT fonte, ano_mes, status, linhas, observacao
FROM workspace.antt_gold.cobertura_fonte_mes
WHERE status <> 'ok'
ORDER BY fonte, ano_mes

-- COMMAND ----------

-- 5c. Viagens sinalizadas por mês (flags de qualidade da fato de viagens)
SELECT detalhe AS mes, regra AS flag, violados, avaliados, pct_violado
FROM workspace.antt_gold.agg_qualidade_registros
WHERE origem = 'flag_por_mes'
ORDER BY detalhe, regra

-- COMMAND ----------

-- 5d. Registros em quarentena por motivo (retirados da Silver e fora da Gold)
SELECT 'viagem_fretado' AS tabela, motivo, COUNT(*) AS linhas
FROM workspace.antt_silver.quarentena_viagem_fretado
GROUP BY motivo
UNION ALL
SELECT 'parada_fretado' AS tabela, motivo, COUNT(*) AS linhas
FROM workspace.antt_silver.quarentena_parada_fretado
GROUP BY motivo
ORDER BY tabela, linhas DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Qual é a evolução mensal do volume transportado?
-- MAGIC
-- MAGIC **Tabela:** `agg_evolucao_mensal_volume` (uma linha por mês).
-- MAGIC
-- MAGIC | Coluna | O que é |
-- MAGIC |---|---|
-- MAGIC | `ano_mes` | Mês (`2026-03`). |
-- MAGIC | `viagens_realizadas` | Viagens registradas pelo Monitriip (fretado), pela data de início. Vazio em mês ausente. |
-- MAGIC | `viagens_licenciadas` | Viagens autorizadas (SISAUT + contínuo). Existe em todos os meses. |
-- MAGIC | `passageiros_licenca` | Passageiros declarados nas licenças das viagens realizadas (cada licença uma vez, no mês da primeira viagem). |
-- MAGIC | `bilhetes_transportados` | Bilhetes (linhas regulares) cujo **mês da viagem** é o mês da linha. É o *volume transportado* do regular. Vazio quando o mês não é confiável (ver `status_mes_viagem`). |
-- MAGIC | `status_mes_viagem` | `confiavel` = mês com cobertura suficiente; `incompleto_inicio` = primeiro mês da janela (faltam os bilhetes vendidos antes de out/2025); `agendado` = viagem futura, ainda não realizada. |
-- MAGIC | `bilhetes_vendidos` | Bilhetes cujo **mês de emissão** (venda) é o mês da linha. É uma série **separada**: mede vendas, não transporte. |
-- MAGIC | `viagens_realizadas_mom_pct` | Variação % de viagens realizadas contra o mês anterior. |
-- MAGIC | `bilhetes_vendidos_mom_pct` | Variação % de bilhetes vendidos contra o mês anterior. |
-- MAGIC
-- MAGIC **Por que duas séries de bilhetes:** um bilhete vendido em dezembro pode ser para viagem em janeiro. A série *transportados* usa o mês da viagem; a série *vendidos* usa o mês da emissão. Em geral a antecedência é curta: pelo menos metade dos bilhetes é vendida no mesmo mês da viagem, e 95% com até 1 mês de antecedência.
-- MAGIC
-- MAGIC **Cuidado:** os arquivos de bilhetes de ago e set/2026 são menores que a média; a queda de ago/2026 deve ser lida com cautela.

-- COMMAND ----------

-- 6a. Séries separadas por mês: viagens, passageiros declarados, bilhetes transportados (mês da viagem) e vendidos (mês de emissão)
SELECT ano_mes, viagens_realizadas, viagens_licenciadas, passageiros_licenca,
       bilhetes_transportados, status_mes_viagem, bilhetes_vendidos,
       viagens_realizadas_mom_pct, bilhetes_vendidos_mom_pct
FROM workspace.antt_gold.agg_evolucao_mensal_volume
ORDER BY ano_mes

-- COMMAND ----------

-- 6b. Qualidade da base de bilhetes por status do mês da viagem
SELECT status_mes_viagem, COUNT(*) AS linhas, SUM(quantidade_bilhetes) AS bilhetes,
       MIN(ano_mes_viagem) AS de, MAX(ano_mes_viagem) AS ate
FROM workspace.antt_gold.fato_bilhete
GROUP BY status_mes_viagem
ORDER BY de
