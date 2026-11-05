# Desenho da implementação: Lakehouse ANTT no Databricks Free

> Status: v2, baseado no perfil real dos arquivos (amostras de set/out 2026). Pendências na seção 11.
> Diagrama: [arquitetura.mmd](arquitetura.mmd) · Perfil bruto: `data/perfil_fontes.json`, `data/perfil_resumo.txt`

## 1. Objetivo e escopo

Pipeline Bronze → Silver → Gold no Databricks Free Edition com os conjuntos da ANTT, regras de qualidade e indicadores que respondam às 6 perguntas de negócio. Entregáveis: notebooks executáveis, documentação de arquitetura e plano de evolução para produção.

- **Janela de dados (decidida):** 12 meses, de **out/2025 a set/2026**, parametrizável (`data_inicio`, `data_fim`).
- **Acesso verificado:** o Databricks Free acessa `dados.antt.gov.br` (API CKAN e downloads) e baixou os 6 conjuntos para o Volume. O portal já ficou indisponível uma vez, então a ingestão precisa de retentativa e de um caminho alternativo por upload manual.
- **Restrições do Free Edition:** só serverless, sem DBFS root, sem init scripts, cotas diárias de computação. Arquivos ficam em Unity Catalog Volumes.

## 2. Fontes (confirmadas)

Todas via CKAN: `https://dados.antt.gov.br/api/3/action/package_show?id=<slug>`. Cada recurso tem `name`, `format`, `last_modified`, `url`.

| Chave | Slug CKAN | Arquivos | Periodicidade | Amostra | Linhas (amostra) |
|---|---|---|---|---|---|
| `viagens_fretado` | `monitriip-servico-fretado-viagens` | 91 CSV | mensal | 1,2 MB | 7.344 |
| `paradas_fretado` | `monitriip-servico-fretado-paradas` | 92 CSV | mensal | 0,8 MB | 5.449 |
| `bilhetes` | `monitriip-bilhetes-de-passagem` | 93 CSV | mensal (mês de emissão) | 18 MB | 132.416 |
| `empresas_fretamento` | `empresas-habilitadas` (recurso "Empresas Fretamento") | CSV+JSON | snapshot mensal | 0,7 MB | ~9.750 |
| `empresas_regular` | `empresas-habilitadas` (recurso "Empresas Regular") | CSV+JSON | snapshot mensal | 27 KB | 408 |
| `licencas_continuo` | `autorizacoes-de-viagem-fretado-continuo` | 4 CSV | anual (2023 a 2026) | 2,8 MB | 8.554 |
| `sisaut` | `sisaut-nacional-internacional` | 82 CSV | mensal | 36 MB | 92.527 |

Observações:
- Os conjuntos "Licenças de Viagem Nacional/Internacional – Serviço Fretado" e "Autorizações ... (Contínuo)" antigos estão marcados como **[Descontinuado]**. As licenças vigentes são `licencas_continuo` (fretamento contínuo) e `sisaut` (eventual e turístico). O PDF do roteiro cita "Licenças de Viagens do Fretamento", e usarei os dois.
- Os demais recursos de cada conjunto (PDF de dicionário, HTML de dashboard, JSON duplicado do CSV) são ignorados. `empresas-habilitadas` também traz Semiurbano, TRIC, Multimodal e Peru, que ficam fora.
- 12 meses carregam cerca de 12 MB de viagens, 10 MB de paradas, 216 MB de bilhetes e 430 MB do SISAUT.

## 3. Perfil dos dados (o que mudou no desenho)

### 3.1 Estrutura por fonte

| Fonte | Colunas principais | Grão |
|---|---|---|
| `viagens_fretado` | `codigo_viagem`, `cnpj`, `placa`, `autorizacao_viagem`, `data_inicio_viagem`, `data_fim_viagem`, `sentido_linha`, `latitude`, `longitude`, `pdop`, `numero_imei`, `in_transbordo`, `codigo_viagem_transbordo` | 1 linha por viagem |
| `paradas_fretado` | `codigo_viagem`, `cnpj`, `placa`, `motivo_parada`, `data_parada`, `latitude`, `longitude`, `autorizacao_viagem`, `sentido_linha` | 1 linha por parada |
| `bilhetes` | `mes_emissao_bilhete`, `mes_viagem`, `ponto_origem_viagem`, `ponto_destino_viagem`, `tipo_servico`, `tipo_gratuidade`, `media_valor_total`, `dp_valor_total`, `quantidade_bilhetes` | **agregado** por mês, origem, destino, serviço e gratuidade |
| `empresas_*` | `razao_social`, `cnpj`, `numero_taf` (fretamento) ou `numero_tar` (regular), `vigencia` | 1 linha por empresa no snapshot |
| `licencas_continuo` | `numero_autorizacao`, `cnpj`, `cnpj_contratante`, `municipio_origem`, `uf_municipio_origem`, `municipio_destino`, `uf_municipio_destino`, `total_passageiros`, `placa`, vigência | 1 linha por autorização e placa |
| `sisaut` | `numero_licenca_viagem`, `cnpj`, `placa`, `municipio_origem`, `uf_município_origem`, `total_passageiros`, `data_inicio_viagem`, `data_fim_viagem`, trechos (`tipo_trecho`, `sequencia_trecho`, município/UF de início e fim, partida e chegada), `granularidade` | 1 linha por **trecho** da licença |

### 3.2 Consequências

1. **Origem e destino não estão nas viagens.** A viagem só tem `cnpj`, `placa`, `autorizacao_viagem` e datas. A origem e o destino vêm das licenças, ligando `autorizacao_viagem` a `numero_licenca_viagem` (sisaut) ou `numero_autorizacao` (contínuo). A normalização é obrigatória: `0008104633` na viagem e `8112812` na licença, então se remove o zero à esquerda e se converte para número.
2. **Bilhetes agregados e sem CNPJ.** Não há ID de bilhete. A duplicidade é definida pelo grão (mês de emissão, mês da viagem, origem, destino, serviço, gratuidade). Bilhetes não entram no ranking de empresas. Respondem origem/destino do transporte regular e volume mensal.
3. **O SISAUT tem várias linhas por licença** (um trecho por linha). Para contar viagens e passageiros, agrego por `numero_licenca_viagem` e pego `total_passageiros` uma vez por licença.
4. **Paradas só existem para parte das viagens** (2.077 `codigo_viagem` distintos contra 7.344 viagens). Número de paradas por viagem pode ser zero e isso é esperado, não erro.
5. **Transbordo:** 3,6% das viagens têm `codigo_viagem_transbordo`. Nas métricas de duração e contagem, trato transbordo como regra explícita (sinalizado, não descartado).

### 3.3 Formatos que exigem tratamento na Bronze/Silver

| Aspecto | Variação encontrada |
|---|---|
| Codificação | `latin1` em viagens, bilhetes, empresas e sisaut. `utf-8` em paradas e licencas_continuo. O arquivo de um mesmo conjunto pode mudar de um mês para outro |
| Separador | `;` em todos |
| Aspas | SISAUT com todos os campos entre aspas |
| Cabeçalho | `uf_município_origem` (acento) no sisaut. `UF_inicio_trecho` em maiúsculas. Normalizo para snake_case sem acento |
| Datas | `dd-MM-yyyy HH:mm:ss` (viagens, paradas), `dd/MM/yyyy` (licenças, empresas), `dd/MM/yyyy HH:mm:ss` (sisaut), `MM/yyyy` (bilhetes) |
| Decimais | vírgula em coordenadas e `pdop` (viagens, paradas). Ponto em bilhetes e distâncias do sisaut |
| CNPJ | só dígitos nas viagens e paradas, com máscara `00.000.000/0001-00` em empresas e licenças |
| Booleano | `in_transbordo` com `SIM`/`NÃO` (acento corrompido sem tratar encoding) |

Qualidade observada na amostra: nulos próximos de zero na maioria das colunas. Exceções: `codigo_viagem_transbordo` 96,4%, `tipo_servico` 2,6% e `tipo_gratuidade` 1,0% nos bilhetes, `UF/município` de trecho 2,7% no sisaut.

## 4. Organização no Unity Catalog

Para evitar conflito de permissão no Free Edition, o catálogo é `workspace` (já existente), com schemas por camada.

| Objeto | Nome | Conteúdo |
|---|---|---|
| Schema | `workspace.antt_raw` | Volume `landing` (arquivos originais) e `ingest_manifest` |
| Schema | `workspace.antt_bronze` | 1 tabela por fonte, colunas string + metadados |
| Schema | `workspace.antt_silver` | entidades tipadas, quarentena, `dq_resultados` |
| Schema | `workspace.antt_gold` | modelo dimensional e agregados |

Volume: `/Volumes/workspace/antt_raw/landing/<chave>/<arquivo>`. O schema `antt_raw` e o Volume `landing` já foram criados pelo notebook de teste.

## 5. Estrutura do repositório

```
antt-lakehouse/
├── databricks.yml                  # bundle: job + targets dev/prod
├── resources/antt_job.yml
├── notebooks/
│   ├── 00_probe_fontes_antt.py     # (existe) conectividade + perfil
│   ├── 01_setup.py                 # schemas, volume, widgets, manifesto
│   ├── 02_ingest_download.py       # CKAN -> Volume, idempotente
│   ├── 03_bronze.py                # Auto Loader por fonte
│   ├── 04_silver_empresas.py
│   ├── 04_silver_licencas.py       # sisaut + continuo -> licenca (por licença)
│   ├── 04_silver_viagens.py
│   ├── 04_silver_paradas.py
│   ├── 04_silver_bilhetes.py
│   ├── 05_gold_modelo.py
│   ├── 06_gold_indicadores.py
│   ├── 07_dq_report.py
│   └── 08_analise.sql
├── src/antt/ (config.py, normalize.py, quality.py, io.py)
├── tests/
└── docs/
```

## 6. Ingestão (02_ingest_download)

1. Para cada fonte, chama `package_show` e filtra os recursos: `format = CSV`, nome com o mês (`Out25`...`Set26`) dentro da janela. Empresas: só "Fretamento" e "Regular". Licenças contínuo: anos 2025 e 2026. Os recursos mensais são identificados pelo prefixo `<Mes><AA> - ` no nome (ex.: `Set26 - Bilhete de Passagem`).
2. Baixa em streaming para o Volume com retentativa e backoff (o portal já oscilou). Arquivo cuja `last_modified` mudou é rebaixado.
3. Registra no `ingest_manifest`: fonte, recurso, url, `last_modified`, bytes, sha256, data do download, status.
4. Se o portal falhar, o notebook usa os arquivos já presentes no Volume (upload manual).

## 7. Bronze (03_bronze)

- Leitura **arquivo a arquivo** com `spark.read` (não Auto Loader), porque a codificação é detectada por arquivo. Idempotente por `_source_file`.
- Todas as colunas como **string**, sem inferência. Opções: `sep=';'`, `quote='"'`, `multiLine=false`.
- **Codificação por arquivo:** detecto `utf-8` ou `latin1` na leitura do primeiro bloco e passo a opção `encoding` por arquivo (o notebook de perfil já faz isso). A mudança de layout entre meses cai em `_rescued_data` e em `addNewColumns`.
- Cabeçalho normalizado (minúsculas, sem acento, snake_case).
- Metadados: `_source_file`, `_file_mod_time`, `_ingest_ts`, `_fonte`, `_row_hash` (sha2 das colunas de negócio).
- Tabelas: `bronze_viagens_fretado`, `bronze_paradas_fretado`, `bronze_bilhetes`, `bronze_empresas`, `bronze_licencas_continuo`, `bronze_sisaut`. Somente append.

## 8. Silver

### 8.1 Padronização (`normalize.py`)
- CNPJ: só dígitos, `lpad` 14, validação do dígito verificador.
- `autorizacao_viagem` / `numero_licenca_viagem` / `numero_autorizacao`: só dígitos, conversão para bigint (remove zeros à esquerda). Chave comum `licenca_id`.
- Datas: parse por formato da fonte, fuso `America/Sao_Paulo`. Decimais: vírgula para ponto.
- Município/UF: maiúsculas, sem acento, trim. UF validada contra as 27. `localidade` (texto livre do SISAUT) não é usada como chave.
- `in_transbordo` para boolean.

### 8.2 Entidades e chaves

| Tabela silver | Chave natural | Derivações |
|---|---|---|
| `empresa` | `cnpj` | tipo (fretamento/regular), `numero_taf` ou `numero_tar`, vigência. Snapshot mais recente por CNPJ, histórico mensal guardado |
| `licenca` | `licenca_id` | origem do registro (`sisaut` ou `continuo`), cnpj, contratante, município/UF origem e destino, `total_passageiros`, período |
| `licenca_trecho` | `licenca_id` + `sequencia_trecho` | só SISAUT, trechos da viagem |
| `viagem_fretado` | `codigo_viagem` | `duracao_min`, `ano_mes_inicio`, `licenca_id`, flag transbordo |
| `parada_fretado` | `codigo_viagem` + `data_parada` + `motivo_parada` | tempo desde o início da viagem |
| `bilhete_agregado` | mês emissão + mês viagem + origem + destino + serviço + gratuidade | `ano_mes_viagem`, UF de origem/destino extraída de "Cidade/UF" |

Escrita com `MERGE` pela chave natural (reprocessamento idempotente).

Em `licenca`, o SISAUT é agregado por `numero_licenca_viagem` (origem da licença; destino = fim do trecho mais longo diferente da origem, porque o fretamento é em circuito fechado e o último trecho é o retorno; o destino do último trecho fica em `municipio_destino_ultimo_trecho`/`uf_destino_ultimo_trecho`; `total_passageiros` único). O contínuo já vem por autorização.

### 8.3 Regras de qualidade (`quality.py`)

Regras declarativas (tabela, expressão SQL, severidade). Reprovadas vão para `quarentena_<entidade>`, sinalizadas ficam com `dq_flags`.

| Tipo | Regra | Tabela | Ação |
|---|---|---|---|
| Completude | `codigo_viagem`, `cnpj`, `data_inicio_viagem` não nulos | viagem | quarentena |
| Validade | CNPJ com dígito verificador válido | viagem, licença, empresa | sinaliza |
| Validade | UF na lista das 27 | licença, bilhete | sinaliza |
| Consistência | `data_fim_viagem >= data_inicio_viagem` | viagem | quarentena |
| Consistência | duração entre 1 min e 30 dias | viagem | sinaliza `duracao_atipica` |
| Consistência | data de início não posterior à data de publicação do arquivo | viagem | sinaliza |
| Integridade | `cnpj` da viagem existe em `empresa` (fretamento) | viagem | sinaliza `empresa_nao_habilitada` |
| Integridade | `licenca_id` da viagem existe em `licenca` | viagem | sinaliza `sem_licenca` |
| Unicidade | `codigo_viagem` repetido | viagem | dedup: mantém o mais recente (`_ingest_ts`) |
| Unicidade | linha idêntica (`_row_hash`) | todas | dedup |
| Unicidade | grão repetido | bilhete | sinaliza |
| Plausibilidade | `total_passageiros` > 0 e < 100 | licença | sinaliza |
| Plausibilidade | `mes_viagem` anterior a `mes_emissao` | bilhete | sinaliza |

Cada execução grava em `silver.dq_resultados`: execução, tabela, regra, avaliados, violados, %. A dedup conta os descartados.

## 9. Gold

### 9.1 Modelo dimensional
**Dimensões**
- `dim_empresa` (cnpj, razão social, tipo fretamento/regular/ambos, número TAF/TAR, vigência). Origem: `silver.empresa`.
- `dim_localidade` (município, UF, chave normalizada). Origem: origens e destinos de `licenca`, `licenca_trecho` e `bilhete_agregado`.
- `dim_data` (data, ano, trimestre, mês, ano_mes, dia da semana). Gerada.
- `dim_cobertura` (fonte, ano_mes, status `ok`/`ausente`/`parcial`, linhas, % da mediana, observação). Origem: `silver.cobertura_fonte_mes`. Todo indicador mensal junta com ela para trazer `status_cobertura` e `mes_completo`.

**Fatos**
- `fato_viagem` — grão: 1 viagem realizada (`codigo_viagem`, Monitriip). Chaves para empresa, origem, destino (via licença) e data de início. Medidas: duração em minutos, nº de paradas, passageiros da licença, flags de qualidade. Entram exatamente as viagens de `silver.viagem_fretado` (76.802), incluindo a cópia mantida de cada um dos 57 códigos repetidos, que carrega o flag `codigo_repetido_entre_arquivos`. As 57 cópias descartadas ficam em `silver.quarentena_viagem_fretado` e não entram (decisão: ver abaixo).
- `fato_licenca` — grão: 1 licença (`licenca_id` + `origem_registro`, SISAUT e contínuo). Chaves para empresa, origem, destino e data de início. Medidas: viagens licenciadas (1 por licença), passageiros declarados, distância. Sustenta a série "viagens licenciadas", que cobre fev/2026.
- `fato_bilhete` — grão: mês de viagem × origem × destino × serviço × gratuidade (regular, sem CNPJ). Medidas: quantidade de bilhetes, valor médio. Liga só a `dim_localidade` e `dim_data`.

**Decisão sobre códigos de viagem repetidos:** são 57 códigos, cada um com 2 cópias em arquivos de meses diferentes. A Silver mantém uma cópia (a mais recente, flag `codigo_repetido_entre_arquivos`) e envia a outra para `silver.quarentena_viagem_fretado`. As cópias em quarentena **não entram** em `fato_viagem` (76.802 linhas); as 57 cópias mantidas entram, sinalizadas, e são excluídas apenas da duração média (G5). As duas quantidades (57 mantidas, 57 em quarentena) são expostas em `agg_qualidade_registros`. A causa fica pendente de resposta da ANTT (ver `pedido_antt_fala_br.md`, item 5).

### 9.2 Indicadores e as perguntas de negócio

| Pergunta | Tabela gold | Lógica |
|---|---|---|
| Viagens por período | `agg_viagens_periodo` | contagem de `fato_viagem` por dia, semana e mês |
| Empresas com maior volume | `agg_ranking_empresas` | viagens e passageiros (licença) por empresa, `rank()` por mês e total. Bilhetes ficam fora (sem CNPJ) |
| Principais origens e destinos | `agg_origem_destino` | top N por município/UF e por par O-D, em duas visões: fretado (via licença) e regular (bilhetes) |
| Duração média | `agg_duracao_media` | média, mediana e p90 de `duracao_min` por mês e empresa, excluindo viagens com `duracao_atipica` |
| Duplicados ou inconsistentes | `agg_qualidade_registros` | de `dq_resultados` e quarentena: % por regra, tabela e mês |
| Evolução mensal do volume transportado | `agg_evolucao_mensal_volume` | por mês: viagens fretado, passageiros declarados nas licenças e bilhetes vendidos, com variação MoM e YoY. Os três ficam em colunas separadas |

Liquid clustering por `ano_mes`.

## 10. Orquestração e produção

Job único (Asset Bundle): `01_setup → 02_ingest → 03_bronze → 04_silver_* (empresas e licenças antes de viagens) → 05_gold_modelo → 06_gold_indicadores → 07_dq_report`. Parâmetros: `data_inicio`, `data_fim`, `fontes`, `full_refresh`.

Evolução para produção (`docs/producao.md`):
- Ambientes dev/prod por Asset Bundle, CI/CD com GitHub Actions, testes unitários das regras de normalização.
- Migrar Bronze/Silver para Lakeflow Declarative Pipelines com `expect`.
- Ingestão incremental agendada (mensal, após a publicação de cada arquivo) com detecção de arquivo reprocessado pelo `last_modified`. Se o portal bloquear, usar proxy ou agendador na região.
- Governança com grants no Unity Catalog, lineage e tags de dados pessoais (placa, `cnpj_cpf_contratante` quando for CPF).
- Observabilidade: alertas SQL quando % de reprovação passar do limite, falha do job notificada.
- Contratos de dados por fonte (colunas e tipos esperados). Mudança de layout gera alerta.
- `OPTIMIZE`/`VACUUM` agendados e dashboard no Databricks SQL.

## 11. Premissas e pendências

1. **Perfil de `empresas_fretamento`:** baixei amostra local (`data/amostras/empresas_fretamento`, colunas `razao_social;cnpj;numero_taf;vigencia`). Falta perfilar no Databricks (nulos, duplicidade de CNPJ).
2. **Ligação viagem ↔ licença:** a amostra indica que `autorizacao_viagem` casa com `numero_licenca_viagem`, mas a taxa de casamento ainda não foi medida. Será a primeira verificação da Silver. Se for baixa, a ligação alternativa é `cnpj` + `placa` + data.
3. **`mes_viagem` dos bilhetes** pode ser futuro (a amostra de set/2026 tem 14 meses de viagem distintos). O volume mensal usa `mes_viagem` para viagens realizadas e `mes_emissao_bilhete` para vendas. Preciso confirmar com o negócio qual das duas responde "volume transportado".
4. **CPF do contratante** (`cnpj_cpf_contratante` no SISAUT) pode ser dado pessoal. Na Gold, mascaro ou omito.
5. **Retrocompatibilidade de layout:** só vi uma amostra por fonte. A leitura de um arquivo antigo (ex.: out/2025) pode ter colunas diferentes. O primeiro `03_bronze` roda sobre os 12 meses e o `_rescued_data` mostra as divergências.

## 12. Ajustes por camada

Os achados da carga real (períodos ausentes, paradas parciais, códigos repetidos, texto corrompido) e o tratamento previsto em cada camada estão em [ajustes_por_camada.md](ajustes_por_camada.md). A Silver e a Gold devem implementar os itens `S*` e `G*` daquele documento.
