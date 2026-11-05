# Lakehouse ANTT: transporte rodoviário interestadual de passageiros

Pipeline Bronze → Silver → Gold no Databricks Free Edition, com dados abertos da ANTT (janela de 12 meses: out/2025 a set/2026).

## Documentação

| Documento | Conteúdo |
|---|---|
| [docs/desenho-implementacao.md](docs/desenho-implementacao.md) | Arquitetura, fontes, camadas, regras de qualidade, modelo dimensional |
| [docs/arquitetura.mmd](docs/arquitetura.mmd) | Diagrama (Mermaid) |
| [docs/ingestao_bronze_resultado.md](docs/ingestao_bronze_resultado.md) | Resultado da ingestão e da Bronze |
| [docs/silver_resultado.md](docs/silver_resultado.md) | Resultado da Silver, ligação viagem↔licença, investigação de códigos repetidos |
| [docs/gold_resultado.md](docs/gold_resultado.md) | Resultado da Gold e **respostas às 6 perguntas de negócio** |
| [docs/ajustes_por_camada.md](docs/ajustes_por_camada.md) | Achados nos dados e tratamento por camada (estado de cada item) |
| [docs/producao.md](docs/producao.md) | Como a solução evoluiria para produção |
| [docs/pedido_antt_fala_br.md](docs/pedido_antt_fala_br.md) | Minuta de pedido à ANTT sobre meses ausentes ou incompletos |

## Ordem de execução dos notebooks (`notebooks/`)

Todos aceitam os parâmetros `catalog` (padrão `workspace`), `data_inicio` (`2025-10`) e `data_fim` (`2026-09`). São idempotentes: reexecutar não duplica dados.

| Ordem | Notebook | O que faz |
|---|---|---|
| 1 | `01_setup` | Cria schemas, Volume e a tabela de manifesto |
| 2 | `02_ingest_download` | Lê o CKAN da ANTT e baixa os CSV da janela para o Volume (retentativa, manifesto, meses ausentes registrados) |
| 3 | `03_bronze` | Carrega os CSV em tabelas Delta, tudo como texto, com metadados e detecção de codificação por arquivo |
| 4 | `04_silver_cobertura`, `04_silver_empresas`, `04_silver_licencas`, `04_silver_viagens`, `04_silver_paradas`, `04_silver_bilhetes` | Tipagem, padronização, deduplicação, quarentena, regras de qualidade (nessa ordem) |
| 5 | `04_silver_dq` | Consolida os resultados das regras em `dq_resultados` |
| 6 | `05_gold_dimensoes`, `05_gold_fatos` (ou `05_gold_run_all`) | Dimensões e fatos |
| 7 | `06_gold_indicadores` (ou `06_gold_run_all`) | As 6 tabelas de indicadores |
| 8 | `08_analise.sql` | Consultas que respondem às 6 perguntas |

Verificações e diagnósticos: `95_verificacao_gold`, `95_verificacao_gold_agg`, `95_verificacao_consultas`, `96_teste_b1`, `97_diag_periodos`, `98_diag_sisaut`, `99_verificacao_bronze`. O `00_probe_fontes_antt` testa o acesso ao portal e levanta o perfil dos arquivos.

## Como rodar

**No Databricks:** importe a pasta `notebooks/` no workspace (Workspace → Import), conecte ao compute serverless e execute na ordem acima.

**Pela linha de comando (Windows):** com a Databricks CLI instalada e autenticada (`databricks auth login --profile antt`), os scripts em `scripts/` importam e executam um notebook:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\executar_notebook_databricks.ps1 -Nb 03_bronze -Tag bronze1
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\executar_gold.ps1 -Tag gold1             # dimensões + fatos + verificação, 2 rodadas
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\executar_gold_indicadores.ps1 -Tag ind1   # indicadores + verificação, 2 rodadas
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\validar_consultas.ps1 -Tag q1             # executa as consultas de 08_analise.sql
```

Cada execução grava o log em `data\_log_<Tag>.txt` e o resultado em `data\_res_<Tag>.json`.

## Job mensal (Lakeflow Jobs)

O pipeline inteiro roda como um Job serverless chamado **`ANTT Lakehouse - mensal`** (`job_id` 193583361732032), definido em `scripts/job_antt_lakehouse.json`. São 13 tarefas de notebook que cobrem ingestão → bronze → silver → gold → verificações. Agenda ativa (UNPAUSED): dia 5 de cada mês às 06:00 (`America/Sao_Paulo`, cron Quartz `0 0 6 5 * ?`). As tarefas passam `catalog=workspace`; os demais widgets (`data_inicio`, `data_fim`) usam os padrões dos notebooks. Só há agendamento mensal — não há gatilho por chegada de arquivo (file arrival).

Dependências entre tarefas (as três silver de base rodam em paralelo; as três silver de fato também):

```
ingest_download → bronze → ┬ silver_cobertura ┐
                           ├ silver_empresas  ┤→ ┬ silver_viagens  ┐
                           └ silver_licencas  ┘  ├ silver_paradas  ┤→ silver_dq
                                                 └ silver_bilhetes ┘
silver_dq → gold_run_all (05_gold_run_all) → gold_indicadores_run_all (06_gold_run_all)
          → verificacao_gold e verificacao_gold_agg (em paralelo)
```

As tarefas `gold_run_all` e `gold_indicadores_run_all` são os notebooks orquestradores `05_gold_run_all` e `06_gold_run_all`, que por dentro executam dimensões, fatos, indicadores e verificações (duas rodadas, para provar idempotência).

Criar ou atualizar o job (idempotente: procura pelo nome e usa `jobs reset` se já existir; senão `jobs create`):

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\criar_job.ps1
```

Rodar uma vez, fora da agenda: `databricks jobs run-now 193583361732032 --profile antt` (a CLI fica em `$env:LOCALAPPDATA\databricks-cli\databricks.exe`). O job usa os notebooks já importados em `/Users/uilian.senna@gmail.com/antt-lakehouse/`; depois de alterar um notebook local, importe-o antes com os scripts abaixo.

Scripts de importação e execução avulsa (`scripts/`):

| Script | Uso |
|---|---|
| `importar_notebooks.ps1` | Importa todos os notebooks de `notebooks/` para o workspace |
| `importar_um_notebook.ps1 -Arquivo <nome.py>` | Importa um notebook (a conferência de conteúdo no fim é específica do `08_analise.sql`) |
| `executar_notebook_databricks.ps1` | Importa e executa um notebook em run avulsa serverless (`jobs submit`) |
| `criar_job.ps1` | Cria/atualiza o Job mensal |

## Estrutura

```
notebooks/   notebooks Databricks (Python e SQL)
scripts/     scripts PowerShell para executar notebooks pela CLI e testar o acesso ao portal
docs/        documentação
data/        perfil das fontes, lista de recursos do portal e amostras locais (não são dados de produção)
```

## Limitações conhecidas

Fevereiro/2026 não foi publicado pela ANTT para viagens e paradas; as paradas de agosto/2026 estão incompletas; empresas regular de março/2026 só existe em JSON. A solução marca esses meses (`ausente` ou `parcial`), em vez de preencher ou zerar. Detalhes em `docs/ajustes_por_camada.md`.
