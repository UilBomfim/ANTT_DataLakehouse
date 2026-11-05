# Resultado da camada Silver

Janela out/2025 a set/2026, catálogo `workspace`, schema `antt_silver`, Databricks Free (serverless). Notebooks em `/Users/uilian.senna@gmail.com/antt-lakehouse/`. Todos os números abaixo vêm da saída dos notebooks (`dbutils.notebook.exit`) nas execuções de verificação.

Ordem de execução: `04_silver_cobertura` e `04_silver_empresas` (já existentes) → `04_silver_licencas` → `04_silver_viagens` → `04_silver_paradas` → `04_silver_bilhetes` → `04_silver_dq`.

## 1. Linhas por tabela

| Tabela | Linhas | Grão / chave natural |
|---|---|---|
| `cobertura_fonte_mes` | 72 | fonte + ano_mes |
| `empresa` | 10.869 | cnpj |
| `empresa_historico` | 117.597 | cnpj + snapshot_mes + tipo_empresa |
| `licenca` | 409.397 (sisaut 408.900, contínuo 497) | licenca_id + origem_registro |
| `licenca_trecho` | 1.108.220 | licenca_id + sequencia_trecho |
| `viagem_fretado` | 76.802 | codigo_viagem |
| `quarentena_viagem_fretado` | 57 | linhas descartadas com `motivo` |
| `parada_fretado` | 95.800 | codigo_viagem + data_parada + motivo_parada |
| `quarentena_parada_fretado` | 0 | |
| `bilhete_agregado` | 1.932.894 | mes_emissao, mes_viagem, origem, destino, tipo_servico_norm, tipo_gratuidade_norm (não único, ver seção 8) |
| `dq_resultados` | 47 regras em 10 tabelas | tabela + regra |

Reconciliação: viagens 76.859 (Bronze) = 76.802 + 57 (quarentena). Bilhetes 1.932.894 = 1.932.894. Paradas 102.884 = 95.800 + 4.326 (`_row_hash` idêntico) + 2.758 (chave natural repetida) + 0 (quarentena).

## 2. Cobertura (S1)

`cobertura_fonte_mes` tem 72 linhas (6 fontes × 12 meses): 68 `ok`, 3 `ausente`, 1 `parcial`.

- `ausente`: viagens 2026-02, paradas 2026-02, empresas_regular 2026-03 (o CKAN não publica o recurso).
- `parcial`: paradas 2026-08 (arquivo com 180 linhas na Bronze, menos de 25% da mediana dos demais meses).
- `parada_fretado.mes_parcial` marca as 180 paradas desse arquivo.

## 3. Ligação viagem → licença (S6)

Taxa total: **99,24%** (76.217 de 76.802 viagens têm `licenca_id` em `silver.licenca`). Todas ligam pelo SISAUT, nenhuma pelo contínuo (0). `licenca_id` nulo na viagem: 0. Sem licença: 585 viagens (flag `sem_licenca`).

| Mês de início | Viagens | Ligadas | Taxa |
|---|---|---|---|
| 2025-10 | 8.681 | 8.584 | 98,9% |
| 2025-11 | 8.561 | 8.550 | 99,9% |
| 2025-12 | 7.159 | 7.151 | 99,9% |
| 2026-01 | 6.485 | 6.454 | 99,5% |
| 2026-02 | sem arquivo | | |
| 2026-03 | 5.792 | 5.786 | 99,9% |
| 2026-04 | 6.858 | 6.667 | 97,2% |
| 2026-05 | 6.467 | 6.248 | 96,6% |
| 2026-06 | 6.066 | 6.058 | 99,9% |
| 2026-07 | 7.065 | 7.063 | 100,0% |
| 2026-08 | 6.324 | 6.322 | 100,0% |
| 2026-09 | 7.344 | 7.334 | 99,9% |

A taxa passou de 50%, então a ligação alternativa por cnpj + placa + data não foi implementada. A chave `licenca_id = cast(regexp_replace(x,'\D','') as bigint)` funciona: `autorizacao_viagem` (com zeros à esquerda) casa com `numero_licenca_viagem`.

## 4. Investigação: `codigo_viagem` repetido (S3)

57 códigos (faixa 8.488.389 a 8.583.171), todos com exatamente 2 cópias, todas em arquivos de meses diferentes (nenhuma dentro do mesmo arquivo).

O que as cópias têm em comum e o que difere:
- Mesmo `cnpj`, mesma `placa` e mesma licença nos 57 casos.
- `data_inicio` e `data_fim` diferem nos 57 casos. Nenhum par é idêntico nos campos de negócio.
- O mês de início de cada cópia coincide com o mês do arquivo onde ela está (o que a Bronze já mostrava).
- Pares de arquivos: 2026-04+2026-05 (19), 2025-10+2025-11 (9), 2026-07+2026-08 (7), 2026-01+2026-07 (7), 2026-05+2026-06 (5), 2025-10+2026-05 (2), 2026-06+2026-07 (2), 2026-01+2026-09 (2), e 1 caso em cada um de 2025-11+2026-07, 2025-12+2026-09, 2026-08+2026-09, 2026-03+2026-04.
- Intervalo entre o fim da primeira cópia e o início da segunda: 44 casos de 0 a 60 segundos (continuação imediata, a segunda cópia abre quando a primeira fecha) e 13 casos com mais de 1 dia (o código foi reaproveitado meses depois).

Exemplos (horário de Brasília):

| codigo_viagem | cnpj / placa / licença | Cópia 1 (arquivo) | Cópia 2 (arquivo) |
|---|---|---|---|
| 8488389 | 15549711000197 / OPU9849 / 7707773 | 31/10/2025 23:29:13 → 01/11/2025 21:50:05 (10_2025) | 01/11/2025 21:50:07 → 02/11/2025 23:58:14 (11_2025) |
| 8488407 | 20375162000111 / AME3D60 / 7705762 | 31/10/2025 23:53:44 → 01/11/2025 04:26:54 (10_2025) | 01/11/2025 04:26:56 → 01/11/2025 09:32:20 (11_2025) |
| 8488395 | 16684210000186 / AME6I28 / 7696499 | 26/10/2025 15:13:58 → 20:14:35 (10_2025) | 23/05/2026 23:56:26 → 23:56:58 (05_2026) |

**Conclusão:** não são o mesmo registro republicado. São viagens distintas que compartilham o código: em 44 casos a segunda cópia continua a primeira (provável encadeamento por transbordo ou reinício do registro, com `in_transbordo` igual nas duas), em 13 o código foi reutilizado meses depois. A regra pedida (manter o de maior `_file_mod_time`) elimina 57 viagens reais (0,07%). Elas ficam em `quarentena_viagem_fretado` com `motivo = codigo_repetido_entre_arquivos` e `arquivo_mantido`. Alternativa recomendada para uma revisão do desenho: chave `codigo_viagem` + `data_inicio`, o que manteria as 57. Fica como decisão de negócio.

## 5. Reparo do texto corrompido do SISAUT (S2)

Reparo por valor: volta cada caractere ao byte original e decodifica como UTF-8, até 2 passadas, aceitando só se não sobrar padrão de mojibake (`Ã` ou `Â` seguido de caractere 0x80-0xFF ou de caractere típico de cp1252 como `ƒ` e `Š`, ou `U+FFFD`). O texto reparado vai para maiúsculas (a dupla codificação deixava `ç` e `ã` minúsculos no meio da palavra). O valor original fica em `*_original`.

Linhas de `licenca_trecho` (1.108.220):

| Coluna | Corrigidas | Continuam inválidas | Valores distintos corrigidos / inválidos |
|---|---|---|---|
| `contratante` | 12.756 | 4.042 | 2.755 / 851 |
| `localidade_inicio_trecho` | 8.635 | 1.848 | 4.070 / 980 |
| `localidade_final_trecho` | 218 | 32 | 195 / 24 |

- Aceitos com padrão de mojibake residual: 0 nas três colunas.
- **Desvio da regra literal "sem Ã/Â":** os textos aceitos contêm `Ã` legítimo (SÃO, UNIÃO, GUIMARÃES): 4.527 em `contratante`, 3.004 em `localidade_inicio_trecho`, 92 em `localidade_final_trecho`. A regra literal rejeitaria reparos corretos. Entre eles, os que têm `Ã`/`Â` seguido de algo que não é letra A-Z (possível resíduo) são 269, 98 e 4.
- As inválidas são dupla ou tripla codificação com bytes perdidos (ex.: `SÃÆÃÂO PAULO FUTEBOL CLUBE`, `PRAÃÂ¡A CENTRAL`). Permanecem como vieram. Em `silver.licenca`, 1.379 licenças têm `contratante` inválido.
- Limite conhecido: um byte perdido que deixa `Ã` seguido de letra ASCII (ex.: `PRAÃA`) é indistinguível de `Ã` legítimo e não é detectado.

## 6. Qualidade (`dq_resultados`)

47 regras em 10 tabelas, 24 com violações (resumo; severidade entre parênteses):

| Tabela | Regra | Avaliados | Violados | % |
|---|---|---|---|---|
| viagem_fretado | duracao_atipica (alerta) | 76.802 | 1.017 | 1,32 |
| viagem_fretado | sem_licenca (alerta) | 76.802 | 585 | 0,76 |
| viagem_fretado | codigo_repetido_entre_arquivos (alerta) | 76.802 | 57 | 0,07 |
| viagem_fretado | empresa_nao_habilitada (alerta) | 76.802 | 2 | 0,003 |
| quarentena_viagem_fretado | motivo_codigo_repetido_entre_arquivos (crítica) | 76.859 | 57 | 0,07 |
| parada_fretado | data_parada_em_mes_diferente_do_arquivo (info) | 95.800 | 4.127 | 4,31 |
| parada_fretado | mes_parcial (info) | 95.800 | 180 | 0,19 |
| bilhete_agregado | mes_viagem_antes_emissao (alerta) | 1.932.894 | 9.303 | 0,48 |
| bilhete_agregado | uf_invalida (alerta) | 1.932.894 | 256 | 0,013 |
| licenca | contratante_texto_corrompido_nao_reparado (alerta) | 409.397 | 1.379 | 0,34 |
| licenca | total_passageiros_fora_de_1_a_99 (alerta) | 409.397 | 116 | 0,03 |
| licenca | total_passageiros_nao_constante_na_licenca (alerta) | 409.397 | 48 | 0,01 |
| licenca | data_fim_antes_data_inicio (alerta) | 409.397 | 5 | 0,001 |
| licenca | uf_invalida (alerta) | 409.397 | 1 | 0,0002 |
| licenca_trecho | uf_inicio_invalida / uf_final_invalida (alerta) | 1.108.220 | 25.837 / 25.838 | 2,33 |
| licenca_trecho | contratante / localidade_inicio / localidade_final não reparados (alerta) | 1.108.220 | 4.042 / 1.848 / 32 | 0,36 / 0,17 / 0,003 |
| licenca_trecho | texto_reparado_* (info) | 1.108.220 | 12.756 / 8.635 / 218 | |
| cobertura_fonte_mes | mes_ausente / mes_parcial (info) | 72 | 3 / 1 | |

Sem violação (23 regras), entre elas: `chave_unica` em todas as tabelas, `cnpj_digito_verificador_invalido` em empresa, `cpf_completo_exposto` em licenca, `viagem_nao_encontrada` em parada_fretado, `fim_antes_do_inicio` e `campo_obrigatorio_nulo` na quarentena de viagens, `grao_repetido` e `mes_viagem_futuro_mais_de_12_meses` em bilhete_agregado.

Observações dos dados:
- Os 25.837 trechos com UF inválida (2,3%) combinam com os 2,7% de UF/município nulos no trecho que o perfil já mostrava; a causa não foi aprofundada (não confirmei se são trechos fora do Brasil).
- Bilhetes com `uf_invalida`: 256 linhas em que o destino vem sem UF (`origem` válida, `uf_destino` vazia).
- Viagens `empresa_nao_habilitada`: 2.

## 7. CPF mascarado (S8)

`cnpj_cpf_contratante` em `silver.licenca`: 239.474 CPFs gravados como `***.***.***-XX` (2 últimos dígitos), 169.866 CNPJs de 14 dígitos, 57 sem documento. **CPFs completos de 11 dígitos em `silver.licenca`: 0** (conferido por regex `^\d{11}$` e `\d{3}\.\d{3}\.\d{3}-\d{2}`; a regra `cpf_completo_exposto` do `dq_resultados` também deu 0 violações). `licenca_trecho` não carrega o documento. A própria fonte já mascara parte dos CPFs (633.296 linhas da Bronze no formato `123.***.***-45`): a Silver descarta também os 3 primeiros dígitos que a fonte deixa visíveis.

Decisão: contratante também aparece mascarado na fonte para pessoa física (`JOS*** ***** ***ÇA`); mantido como veio.

## 8. Decisões tomadas

- **Escrita por overwrite determinístico** em todas as tabelas novas, com a justificativa no cabeçalho de cada notebook: as tabelas são derivadas por completo da Bronze e as flags dependem de outras tabelas Silver que também são reconstruídas. Reexecutar dá o mesmo resultado.
- **Contínuo: 1 linha por `numero_autorizacao`** com `qtd_placas` (máximo 152), porque a viagem liga pela autorização e uma linha por placa multiplicaria origem, destino e passageiros. 17.421 linhas da Bronze viram 497 licenças. Se a mesma autorização aparece nos arquivos de 2025 e 2026, vale a vigência mais recente.
- **SISAUT:** origem da licença = `municipio_origem`/`uf_municipio_origem`. **Destino (regra revisada na Gold):** fim do trecho mais longo cujo município/UF difere da origem; se todos terminam na origem, o destino é a origem. A regra inicial (município final do trecho de maior `sequencia_trecho`) devolvia a origem em 408.856 de 408.900 licenças, porque o fretamento é em circuito fechado e o último trecho é o retorno; com a regra nova são 193. O destino do último trecho continua disponível em `municipio_destino_ultimo_trecho`/`uf_destino_ultimo_trecho`. 30 linhas do SISAUT estão deslocadas (data no lugar do número da licença) e são descartadas na Silver. Trechos repetidos entre arquivos para a mesma (licença, sequência): 125, fica o mais recente.
- **`total_passageiros`:** 48 das 408.900 licenças SISAUT não têm valor constante entre os trechos. Adotado o maior valor e a licença fica marcada com `pax_nao_constante`.
- **Dedup de viagens:** `_file_mod_time` desc, depois `_ingest_ts` desc, depois nome do arquivo. O desenho citava só `_ingest_ts`, que é idêntico dentro de uma mesma carga.
- **Paradas:** dedup por `_row_hash` (4.326) e depois pela chave natural (2.758). `ano_mes_parada` vem de `data_parada`. 6 paradas têm data de 2025-03, 2025-06 e 2025-08 e 44 de 2026-10 (fora da janela); ficam, com o mês real.
- **Bilhetes:** `mes_viagem_futuro` = mais de 12 meses após a emissão. Resultado: 0 linhas (o maior mês de viagem é 2027-07, com emissão até 2026-09). Não há grão repetido (0 linhas), então o grão da Silver é único na prática, mas a tabela não impõe a unicidade. `tipo_servico` e `tipo_gratuidade` nulos viram `NAO_INFORMADO` nas colunas `*_norm` (60.466 e 15.062 linhas); os originais ficam nas colunas sem sufixo.
- **`dq_resultados`** guarda só a execução mais recente (overwrite). Histórico por append duplicaria linhas a cada reexecução; fica para a orquestração.
- Datas do SISAUT conferidas com a Bronze: `dd/MM/yyyy` em data de solicitação, início e fim; `dd/MM/yyyy HH:mm:ss` em autorização e horários de trecho. 18 licenças sem `data_fim` e 1 trecho sem partida (campos vazios na origem).

## 9. Divergências do desenho (seção 8 do `desenho-implementacao.md`)

- A seção 8.2 prevê `MERGE` pela chave natural; foi usado overwrite (justificado acima).
- `licenca` tem chave `licenca_id` + `origem_registro` (a seção 8.2 diz só `licenca_id`). Nenhum `licenca_id` aparece nas duas origens (0), então na prática `licenca_id` é único.
- Regra "data de início não posterior à data de publicação do arquivo" (viagem) e validação de dígito verificador de CNPJ em viagem e licença (seção 8.3) não foram implementadas; o dígito verificador só é validado em `empresa`. O CNPJ de viagem e licença é normalizado para 14 dígitos.
- A regra de `codigo_viagem` repetido mantém o registro mais recente como no desenho, mas a investigação mostra que são viagens distintas (seção 4).
- Plausibilidade `total_passageiros` > 0 e < 100 sinaliza 116 licenças com 100 passageiros ou mais.
- Reparo de mojibake aceita `Ã` legítimo (seção 5).

## 10. Verificação

| Item | Resultado |
|---|---|
| (a) Contagens > 0 | licenca 409.397, licenca_trecho 1.108.220, viagem_fretado 76.802, parada_fretado 95.800, bilhete_agregado 1.932.894, dq_resultados 47 |
| (b) Reexecução não muda contagens | licenças (3 execuções, mesmas contagens), viagens (2 execuções), paradas, bilhetes e dq: ver seção 11 |
| (c) CPF completo em `silver.licenca` | 0 |
| (d) Chave única | `licenca` (licenca_id+origem_registro): 0 duplicatas. `licenca_trecho`: 0. `viagem_fretado` (codigo_viagem): 0. `parada_fretado`: 0 |
| (e) Texto reparado aceito sem mojibake | 0 com padrão de mojibake; `Ã` legítimo permanece (seção 5) |
| (f) Taxa de ligação viagem → licença | 99,24% (seção 3) |

## 11. Reexecuções e itens opcionais

**Idempotência das Silver:** licenças (3 execuções), viagens (2), paradas, bilhetes e `dq_resultados` foram reexecutados e as contagens da seção 1 não mudaram.

**I2 (manifesto de meses ausentes):** o `02_ingest_download` registrou 3 meses com `status = ausente_no_portal` (viagens 2026-02, paradas 2026-02, empresas regular 2026-03). A segunda execução não duplicou nada (92 `ok` e 3 `ausente_no_portal` nas duas).

**B1 (arquivo republicado):** a lógica está no `03_bronze` (compara o `_file_mod_time` gravado com o do Volume, apaga as linhas do arquivo e recarrega só ele). Testado de verdade com `notebooks/96_teste_b1.py`, que reescreve com o mesmo conteúdo um CSV pequeno de `empresas_regular` (`empresas_habilitadas_regular_01_2026.csv`, 28.252 bytes) e roda a Bronze duas vezes:

| Etapa | Arquivos recarregados | Linhas em `bronze_empresas` |
|---|---|---|
| Antes | | 118.372 |
| Bronze, 1ª rodada | 1 (só o reescrito) | 118.372 |
| Bronze, 2ª rodada | 0 | 118.372 |

O `_file_mod_time` do arquivo passou de 17:04:36 para 19:29:45. Uma tentativa anterior com `os.utime` não valeu como teste, porque Volumes ignoram a mudança de data; o teste válido é o da reescrita.
