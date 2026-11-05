# Evolução para produção

Este documento descreve como a solução, hoje rodando no Databricks Free Edition com 12 meses de dados (out/2025 a set/2026), evoluiria para um ambiente de produção. Cada seção parte do que existe hoje, aponta a limitação e propõe a mudança.

## 1. Ponto de partida

| Aspecto | Hoje (Free Edition) | Limite que motiva a mudança |
|---|---|---|
| Computação | só serverless, com cota diária | sem controle de custo, de tamanho nem de agendamento fino |
| Ambientes | um workspace, catálogo `workspace` | sem separação entre desenvolvimento e produção |
| Execução | **Job mensal implementado** (`ANTT Lakehouse - mensal`, ver seção 4.1); notebooks avulsos ainda podem ser disparados por script local (CLI) | sem alerta de falha, sem service principal, sem CI/CD |
| Janela | 12 meses (≈ 735 MB de CSV) | a ANTT publica ≈ 91 meses; o histórico completo é ≈ 10 vezes maior |
| Ingestão | download direto do portal, com retentativa | o portal já ficou indisponível e bloqueou conexões automatizadas |
| Qualidade | regras em código Python/SQL próprio | sem expectativas declarativas nem painel de acompanhamento |
| Código | arquivos em uma pasta local | sem repositório, revisão nem teste automatizado |

## 2. Arquitetura alvo

```
Portal ANTT (CKAN)
   │  agendador mensal + checagem diária de republicação
   ▼
Volume raw (Unity Catalog)  ─► Bronze (Delta, append, tudo string)
                                 │  Lakeflow Declarative Pipelines + expectativas
                                 ▼
                               Silver (tipada, deduplicada, quarentena, cobertura)
                                 │
                                 ▼
                               Gold (dimensões, fatos, indicadores)
                                 │
                    ┌────────────┴─────────────┐
                    ▼                          ▼
             Dashboard (SQL)            Genie / consumo por API SQL
```

A divisão em camadas, o catálogo `antt` (um por ambiente), os schemas `raw`, `bronze`, `silver` e `gold` e as tabelas atuais continuam. O que muda é como o código roda e como é controlado.

## 3. Ambientes, código e implantação

- **Repositório Git** com `notebooks/`, `src/` (funções de normalização e regras de qualidade extraídas dos notebooks) e `tests/`.
- **Databricks Asset Bundles** com dois alvos: `dev` (catálogo `antt_dev`, janela curta de 2 meses) e `prod` (catálogo `antt`, histórico completo). O mesmo código implanta nos dois; só mudam parâmetros e permissões.
- **CI** (GitHub Actions ou equivalente) em cada pull request: lint, testes unitários das funções de normalização (CNPJ, datas, decimais, reparo de texto, regra de destino da licença) e um teste de integração em `antt_dev` com uma amostra pequena.
- **CD**: merge na branch principal implanta o bundle em `dev`; promoção para `prod` com aprovação manual.
- Os orquestradores atuais (`05_gold_run_all`, `06_gold_run_all`) viram tarefas de um job, e os scripts `.ps1` deixam de existir.

## 4. Orquestração e ingestão

### 4.1 Já implementado: Job mensal (Lakeflow Jobs)

- Job `ANTT Lakehouse - mensal` (`job_id` 193583361732032), serverless, definido em `scripts/job_antt_lakehouse.json` e criado/atualizado de forma idempotente por `scripts/criar_job.ps1` (busca pelo nome; `jobs reset` se existir, `jobs create` se não).
- 13 tarefas de notebook com as dependências lidas no código:
  - `ingest_download` → `bronze`
  - `bronze` → `silver_cobertura`, `silver_empresas`, `silver_licencas` (em paralelo)
  - as três silver acima → `silver_viagens`, `silver_paradas`, `silver_bilhetes` (em paralelo)
  - as três silver de fato acima → `silver_dq`
  - `silver_dq` → `gold_run_all` (notebook `05_gold_run_all`, que roda dimensões + fatos + verificação, duas vezes para provar idempotência)
  - `gold_run_all` → `gold_indicadores_run_all` (notebook `06_gold_run_all`)
  - `gold_indicadores_run_all` → `verificacao_gold` e `verificacao_gold_agg` (em paralelo)
- As tarefas `gold_run_all` e `gold_indicadores_run_all` são os orquestradores já existentes (`05_gold_run_all`, `06_gold_run_all`), que internamente chamam `05_gold_dimensoes`, `05_gold_fatos`, `06_gold_indicadores` e as verificações via `dbutils.notebook.run`.
- Agenda ativa (UNPAUSED): dia 5 de cada mês, 06:00, fuso `America/Sao_Paulo` (cron Quartz `0 0 6 5 * ?`).
- As tarefas passam `catalog=workspace`; os demais widgets (`data_inicio=2025-10`, `data_fim=2026-09`) usam os valores padrão dos notebooks.
- A Free Edition só oferece computação serverless. O grafo tem no máximo 3 tarefas em paralelo (as três silver de base, depois as três silver de fato), e `max_concurrent_runs = 1` com fila habilitada evita que uma execução atropele a anterior.
- **Não** há gatilho por chegada de arquivo (file arrival): apenas o agendamento mensal por cron. File arrival permanece como evolução futura (seção 4.2).
- Resultado da run de teste por tarefa: ver seção "Verificação do Job" abaixo.

### 4.2 Ainda é evolução

- **Gatilho por chegada de arquivo (file arrival)** no Volume de landing, como complemento ao agendamento mensal: dispara a ingestão assim que um arquivo novo é publicado, sem esperar o dia 5. Não implementado nesta fase.
- **Agenda mais fina / diária:** os arquivos mensais saem por volta do dia 15 (viagens, paradas), no fim do mês (empresas, licenças) e com atraso variável (bilhetes). Rodar diariamente seria viável porque a ingestão é idempotente: se não há arquivo novo, não faz nada. Hoje o job roda só uma vez por mês (dia 5).
- **Alertas de falha e notificações** (e-mail/chat) e execução por *service principal* em vez da conta do usuário.
- **CI/CD** (Databricks Asset Bundles + GitHub Actions) para versionar e promover o job entre `dev` e `prod`.

### 4.3 Verificação do Job (run de teste)

Após criar o job com `scripts/criar_job.ps1`, uma execução manual (`jobs run-now`, trigger `ONE_TIME`) rodou o pipeline inteiro ponta a ponta. Resultado: **run concluída com `SUCCESS`**, as 13 tarefas terminaram em `SUCCESS`. Duração de execução por tarefa (sem contar fila/setup serverless entre tarefas):

| Tarefa | Resultado | Execução |
|---|---|---|
| ingest_download | SUCCESS | 19 s |
| bronze | SUCCESS | 30 s |
| silver_cobertura | SUCCESS | 15 s |
| silver_empresas | SUCCESS | 26 s |
| silver_licencas | SUCCESS | 72 s |
| silver_viagens | SUCCESS | 46 s |
| silver_paradas | SUCCESS | 23 s |
| silver_bilhetes | SUCCESS | 326 s |
| silver_dq | SUCCESS | 25 s |
| gold_run_all | SUCCESS | 242 s |
| gold_indicadores_run_all | SUCCESS | 188 s |
| verificacao_gold | SUCCESS | 22 s |
| verificacao_gold_agg | SUCCESS | 18 s |

Duração total da run (incluindo fila e provisionamento serverless entre as tarefas): ≈ 16 min. `silver_bilhetes` e as duas tarefas de gold são as mais demoradas (bilhetes é a maior tabela; os orquestradores de gold rodam cada notebook duas vezes para provar idempotência). O agendamento mensal ficou **UNPAUSED**.

Não há gatilho por chegada de arquivo para verificar — essa capacidade não foi implementada (ver 4.2). Também não há notificação de falha nem execução por service principal nesta fase.
- **Republicação:** a ingestão compara a data de modificação publicada no CKAN. A Bronze já recarrega apenas o arquivo republicado (item B1). Isso cobre a hipótese de a ANTT publicar fevereiro/2026 ou corrigir as paradas de agosto/2026.
- **Acesso ao portal:** o portal já bloqueou conexões automatizadas por períodos. Em produção, mover a ingestão para um agendador na região (ou proxy) com User-Agent identificado, retentativa com espera crescente e alerta quando o portal ficar indisponível por mais de um ciclo. Manter o caminho de contingência por upload manual no Volume.
- **Backfill:** parâmetros `data_inicio` e `data_fim` do job. Carregar o histórico completo (≈ 91 meses) em lotes anuais.

## 5. Pipelines declarativos e qualidade de dados

- Migrar Bronze e Silver para **Lakeflow Declarative Pipelines**. As regras de `dq_resultados` viram **expectativas** (`expect` para sinalizar, `expect_or_drop` para quarentena), e o painel de qualidade do pipeline substitui parte do relatório manual.
- Ingestão por **Auto Loader** quando a detecção de codificação por arquivo puder ser feita na leitura. Hoje a codificação (UTF-8 ou latin1) é detectada arquivo a arquivo e a carga é feita com `spark.read`. Em produção, isso exige uma etapa de normalização de codificação antes do Auto Loader, ou uma lista fixa de codificação por fonte, validada por teste de contrato.
- **Contratos de dados** por fonte (colunas, tipos esperados, formato de data, separador). Mudança de layout gera alerta e envia o arquivo para quarentena em vez de quebrar a carga. Nos 12 meses carregados nenhum layout mudou, mas a ANTT publica desde 2019.
- **Cobertura por fonte e mês** (`cobertura_fonte_mes`) vira indicador oficial, com limite configurável de "parcial" (hoje 25% da mediana dos demais meses).

## 6. Governança e segurança

- **Unity Catalog:** grupos para engenharia (escreve raw, bronze e silver), analistas (lê gold) e auditoria (lê tudo). Nenhum usuário escreve na gold fora do job.
- **Identidade de execução:** os jobs rodam com um *service principal*, não com a conta de uma pessoa. A autenticação por navegador usada hoje no desenvolvimento não vai para produção.
- **Dados pessoais:** `silver.licenca` já mascara o CPF do contratante (mantém só os dois últimos dígitos). Também são sensíveis a placa e o nome do contratante pessoa física. Aplicar *tags* de sensibilidade no catálogo e *column masks* para perfis sem permissão. Revisar com o encarregado de dados (LGPD) se a placa deve ir à gold.
- **Segredos:** nenhuma credencial em código. Tokens e chaves em *secret scopes*.
- **Linhagem** do Unity Catalog para rastrear de qual arquivo e qual regra veio cada indicador, e **system tables** para auditoria de acesso.

## 7. Observabilidade

- **Alertas** (e-mail ou chat) para: falha do job, portal indisponível, mês esperado ausente além do prazo, mês `parcial`, e aumento de violações de uma regra acima de um limite (por exemplo, cópias de `codigo_viagem` repetido acima de 0,2% das viagens; hoje 0,07%).
- **Dashboard operacional** com duração das tarefas, linhas por camada, cobertura por fonte e mês, e resultados de `dq_resultados` ao longo do tempo (hoje a tabela guarda só a última execução; em produção, gravar histórico com data de execução).
- **Lakehouse Monitoring** nas tabelas gold para detectar desvio de distribuição, como queda brusca de viagens por mês.

## 8. Desempenho e custo

- **Liquid clustering** em `fato_viagem` e `fato_licenca` por `ano_mes_inicio` e `cnpj`, e em `fato_bilhete` por `ano_mes_viagem`. Hoje as tabelas são pequenas (a maior tem ≈ 1,9 milhão de linhas); com o histórico completo, `fato_bilhete` passa de 15 milhões.
- **`OPTIMIZE` e `VACUUM`** agendados semanalmente; retenção da Bronze definida por política (por exemplo, 24 meses de arquivos brutos no Volume, com a Bronze preservada).
- **Reprocessamento incremental:** hoje a Silver e a Gold usam `overwrite` determinístico, porque a base é pequena. Para o histórico completo, trocar por `MERGE` por chave natural e recalcular só os meses afetados.
- **Computação:** *serverless* para cargas pequenas e frequentes; avaliar *job cluster* para o backfill. Definir orçamento e alerta de custo por job.

## 9. Pontos de atenção conhecidos (dívida técnica)

| Item | Situação | Ação em produção |
|---|---|---|
| Meses ausentes (viagens e paradas de fev/2026, empresas regular de mar/2026) | tratados como lacuna explícita, com nulo e status `ausente` | pedido à ANTT (`pedido_antt_fala_br.md`); a ingestão recarrega sozinha se publicarem |
| Paradas de ago/2026 incompletas (180 linhas) | marcadas `parcial` e fora das métricas | mesma solicitação |
| Códigos de viagem repetidos entre arquivos (57) | uma cópia mantida, a outra em quarentena | esclarecer com a ANTT se o código é reutilizado ou encadeado; ajustar a chave se necessário |
| Destino da licença de fretamento | aproximação: fim do trecho mais longo diferente da origem | validar a regra com especialistas de negócio; guardar o roteiro completo de trechos na gold para análises de rota |
| Bilhetes por mês da viagem | janela de 12 meses perde bilhetes vendidos antes do início; os últimos meses ainda não aconteceram | com histórico completo, o viés some (restam apenas os meses futuros, marcados `agendado`) |
| YoY | nulo na janela atual (12 meses) | passa a existir com 13 meses ou mais |
| Texto corrompido no SISAUT | 2,4% das linhas; parte reparada na Silver, parte fica como veio | reportar à ANTT; manter o reparo e os contadores |
| Duração com cauda longa | média ≈ 37 h, mediana ≈ 13 h | publicar a mediana como indicador principal |

## 10. Roadmap sugerido

1. **Fase 1 (1 a 2 semanas):** repositório Git, bundle com `dev` e `prod`, testes das funções de normalização, job agendado com alertas básicos.
2. **Fase 2 (2 a 4 semanas):** pipelines declarativos com expectativas, contratos de dados por fonte, histórico de qualidade, backfill do histórico completo.
3. **Fase 3 (contínua):** `MERGE` incremental, dashboards operacional e de negócio, Lakehouse Monitoring, tags e máscaras de dados pessoais, revisão periódica das regras com a área de Transportes.

## 11. Critérios para considerar a solução em produção

- Job agendado (**feito**: `ANTT Lakehouse - mensal`), com falha notificada e execução por service principal (**pendente**: o job roda com a conta do usuário e sem notificação).
- Cobertura por fonte e mês acompanhada, com alerta de mês ausente ou parcial.
- Reexecução idempotente comprovada (já verificada em todas as camadas) e backfill testado em `dev`.
- Regras de qualidade com histórico e dono definido na área de negócio.
- Dados pessoais classificados e mascarados conforme a política da organização.
