# Ajustes registrados por camada

Fonte: achados da Bronze (`docs/ingestao_bronze_resultado.md`), da investigação de períodos faltantes e do diagnóstico `notebooks/97_diag_periodos.py`. Cada item diz em que camada será tratado e o estado atual.

Estados: `pendente`, `em andamento`, `feito`, `externo` (depende da ANTT ou de decisão do negócio).

## Achados que originam os ajustes

| Achado | Evidência |
|---|---|
| Viagens e paradas de **fev/2026** não existem no portal | O CKAN não tem o recurso. O histórico de atividade não tem publicação em 15/03/2026 |
| Fevereiro **não** está dentro de arquivos de outros meses | 100% das 76.859 viagens têm mês de início igual ao mês do arquivo. Zero viagens iniciadas em fev/2026 |
| Paradas de fev/2026 em outros arquivos são só o fim de viagens de janeiro | 693 paradas com data de fevereiro (692 no arquivo de janeiro, 1 no de março) contra 15.848 em janeiro |
| **Paradas de ago/2026** incompletas e não recuperáveis | Arquivo com 178 linhas. Nenhum arquivo de set/2026 contém paradas de agosto |
| Qualidade das paradas cai ao longo da janela | Razão paradas/viagens: 2,4 (nov/2025), 0,45 (mai/2026), 0,03 (ago/2026) |
| **57 `codigo_viagem` aparecem em mais de um arquivo**, sem ser a mesma viagem repetida | Defasagem zero para todas as viagens. Precisa investigação |
| Empresas regular de **mar/2026** só tem JSON, igual ao de fevereiro | JSON com 72.322 bytes, mesmo tamanho de fev. JSON de fretamento de março aponta para arquivo `..._02_2026.json` |
| Texto corrompido no SISAUT | 26.905 linhas (2,4%) em `contratante`, `localidade_inicio_trecho`, `localidade_final_trecho` |
| Bilhetes de ago e set/2026 menores | ~130 mil linhas contra 160 a 180 mil nos meses anteriores |
| Paradas podem ter data no mês seguinte ao do arquivo | 6.419 linhas com defasagem +1 mês. O arquivo é por mês de início da viagem |

## Ingestão (`02_ingest_download`)

| ID | Ajuste | Estado |
|---|---|---|
| I1 | Escolher, por nome de arquivo, o recurso com `last_modified` mais recente (empresas publica vários CSV com o mesmo nome) | feito |
| I2 | Registrar no manifesto os meses esperados da janela sem recurso publicado (`status = ausente_no_portal`), para alimentar a cobertura | feito: 3 meses registrados (viagens 2026-02, paradas 2026-02, empresas regular 2026-03); a segunda execução não duplicou |
| I3 | Ignorar JSON de empresas (fonte única é o CSV) | feito |

## Bronze (`03_bronze`)

| ID | Ajuste | Estado |
|---|---|---|
| B1 | Tratar arquivo **republicado**: controlar por `_source_file` + `_file_mod_time` e substituir as linhas do arquivo quando a data mudar. Hoje um arquivo já carregado é ignorado | feito: teste real (`notebooks/96_teste_b1.py`) reescreveu 1 CSV de empresas_regular; a Bronze recarregou só esse arquivo (1 novo), `bronze_empresas` ficou em 118.372 linhas, o `_file_mod_time` do arquivo mudou e uma segunda rodada recarregou 0 |
| B2 | Preservar o texto corrompido do SISAUT como veio (reparo é na Silver) | feito |
| B3 | Ajustar a seção 7 do desenho: leitura arquivo a arquivo, não Auto Loader | feito |

## Silver

| ID | Ajuste | Estado |
|---|---|---|
| S1 | Criar `silver.cobertura_fonte_mes` (fonte, mês, esperado, publicado, linhas, mediana dos outros meses, % da mediana, status `ok`/`ausente`/`parcial`). Regra de `parcial`: menos de 25% da mediana dos demais meses. Cobre fev/2026 (viagens e paradas, `ausente`), ago/2026 (paradas, `parcial`) e mar/2026 (empresas regular, `ausente`) | feito (72 linhas: 68 ok, 3 ausente, 1 parcial) |
| S2 | Reparar o texto corrompido do SISAUT (`contratante`, `localidade_*`) tentando `encode('latin1').decode('utf-8')` por valor, com registro de quantas linhas foram corrigidas | feito, com resíduo: nos 1.108.220 trechos, 12.756 contratante, 8.635 localidade de início e 218 localidade final corrigidos; 4.042, 1.848 e 32 seguem inválidos (mantêm o original). Detalhe em `silver_resultado.md` |
| S3 | Regra de qualidade para `codigo_viagem` em mais de um arquivo: manter o registro mais recente, sinalizar `codigo_repetido_entre_arquivos` e quantificar. Investigar a causa. **Decisão do projeto: as cópias descartadas dos repetidos ficam na quarentena** (não entram na Gold; a cópia mantida entra com o flag) | feito: 57 códigos, 2 cópias cada, em arquivos de meses diferentes. As cópias são viagens distintas (início e fim diferem), não duplicatas: 44 são continuação imediata (0 a 60 s) e 13 reaproveitam o código meses depois. A regra pedida mantém 1 e envia 57 linhas à quarentena; a chave `codigo_viagem`+`data_inicio` evitaria a perda |
| S4 | Paradas: usar `data_parada` para o mês, não o mês do arquivo; marcar `mes_parcial` nas paradas de arquivo com status `parcial` | feito: 95.800 paradas, 180 com `mes_parcial`, 4.127 com mês da parada diferente do mês do arquivo |
| S5 | Empresas: um registro por CNPJ (snapshot mais recente), unificar `numero_taf`/`numero_tar`; descartar os JSON de mar/2026 | feito: 10.869 CNPJs únicos, 0 duplicatas, 0 CNPJ inválido (histórico com 117.597 linhas) |
| S6 | Licenças: agregar o SISAUT por `numero_licenca_viagem`; normalizar `licenca_id`; **medir a taxa de ligação viagem ↔ licença** e reportar | feito: 408.900 licenças SISAUT e 497 do contínuo; taxa de ligação 99,24% (76.217 de 76.802 viagens), todas pelo SISAUT |
| S7 | Bilhetes: sinalizar `mes_viagem` futuro e `mes_viagem` anterior à emissão | feito: 1.932.894 linhas; 9.303 com viagem antes da emissão; 0 com viagem a mais de 12 meses da emissão |
| S8 | Mascarar CPF em `cnpj_cpf_contratante` (dado pessoal) | feito: 0 CPF completo em `silver.licenca` (239.474 CPFs mascarados como `***.***.***-XX`) |

## Gold

| ID | Ajuste | Estado |
|---|---|---|
| G1 | Em fev/2026 o volume de viagens fica **nulo**, não zero. Todo indicador mensal traz `status_cobertura` e `mes_completo`; variação MoM e YoY pula meses sem cobertura | feito: fev/2026 com `viagens_realizadas` e duração nulas e `status = ausente` em `agg_viagens_periodo`, `agg_duracao_media` e `agg_evolucao_mensal_volume`; MoM de mar/2026 nulo; YoY nulo em toda a janela de 12 meses |
| G2 | Série de apoio **"viagens licenciadas"** (SISAUT + contínuo, por mês de início, a partir de `fato_licenca`) ao lado de "viagens realizadas" (Monitriip), com rótulo claro de que são medidas diferentes. Cobre fev/2026 | feito: `viagens_licenciadas` de fev/2026 = 25.459 (não nulo) nas tabelas de período e de evolução |
| G7 | Criar `fato_licenca` e `dim_cobertura` (modelo na seção 9.1 do desenho) | feito: `fato_licenca` 409.397 linhas, `dim_cobertura` 72 linhas, chaves sem duplicatas |
| G8 | `agg_qualidade_registros` expõe a contagem dos 57 códigos repetidos mantidos na quarentena | feito: `agg_qualidade_registros` traz 57 cópias descartadas (quarentena) e 57 cópias mantidas (com flag) |
| G3 | Excluir paradas de ago/2026 (e meses `parcial`) das métricas de paradas | feito em `fato_viagem`: `qtd_paradas` só conta paradas de arquivo não parcial (soma na fato = 95.620 = 95.800 − 180 da Silver) e `paradas_confiaveis` é falso nos meses parcial/ausente. O roteiro não tem indicador de paradas, então não há `agg_` específico |
| G4 | Bilhetes: volume transportado por **mês da viagem** (`bilhetes_transportados`) e vendas por **mês de emissão** (`bilhetes_vendidos`) em colunas/séries separadas. Nota para ago e set/2026 (arquivos de emissão menores que a média). Meses de viagem futuros (após o último mês de emissão carregado) marcados como `agendado`, fora do volume transportado | feito: p95 da antecedência = 1 mês; `incompleto_inicio` 7,36%, `confiavel` 90,86%, `agendado` 1,78% dos bilhetes; soma por status = total da Silver (34.634.933); séries `bilhetes_transportados` e `bilhetes_vendidos` separadas em `agg_evolucao_mensal_volume`. Detalhes em `gold_resultado.md` |
| G5 | Duração média: excluir viagens com `duracao_atipica` e `codigo_repetido_entre_arquivos` | feito em `agg_duracao_media` (por mês e por empresa×mês) |
| G6 | Expor `agg_qualidade_registros` e a cobertura por fonte e mês como tabela de consumo | feito: `agg_qualidade_registros` (82 linhas) e `cobertura_fonte_mes` (72 linhas) em `antt_gold` |

## Externo e decisões

| ID | Item | Estado |
|---|---|---|
| E1 | Pedido à ANTT pelo Fala.BR: viagens e paradas de fev/2026, republicação das paradas de ago/2026, empresas regular de mar/2026 e esclarecimento dos 57 códigos de viagem repetidos | externo: minuta pronta em `docs/pedido_antt_fala_br.md`, falta o envio pelo solicitante |
| E2 | Definir com o negócio: o "volume transportado" usa mês da viagem ou mês de emissão do bilhete | decidido: **mês da viagem** para volume transportado; as vendas por **mês de emissão** entram como série separada (`bilhetes_vendidos`). Confirmar com o negócio |
| E3 | Se a ANTT publicar fev/2026, `02_ingest` baixa sozinho e B1 garante a recarga correta | pronto (I2 e B1 feitos); falta a ANTT publicar |
