# Resultado da camada Gold

Janela out/2025 a set/2026, catálogo `workspace`, schema `antt_gold`, Databricks Free (serverless). Números vindos da saída dos notebooks (`dbutils.notebook.exit`) nas execuções de verificação.

Ordem: `05_gold_dimensoes` → `05_gold_fatos` → `95_verificacao_gold` → `06_gold_indicadores` → `95_verificacao_gold_agg`. Orquestradores: `05_gold_run_all`, `06_gold_run_all` e, para reprocessar após mudança na Silver de licenças, `07_rerun_destino`.

## 1. Tabelas e grão

| Tabela | Linhas | Grão / chave |
|---|---|---|
| `dim_empresa` | 10.869 | `cnpj` |
| `dim_localidade` | 5.103 | `localidade_key` = MUNICIPIO\|UF |
| `dim_data` | 1.095 | `data` (2025-01-01 a 2027-12-31) |
| `dim_cobertura` | 72 | `fonte` + `ano_mes` |
| `cobertura_fonte_mes` (consumo) | 72 | cópia de `dim_cobertura` |
| `fato_viagem` | 76.802 | `codigo_viagem` |
| `fato_licenca` | 409.397 | `licenca_id` + `origem_registro` |
| `fato_bilhete` | 1.932.894 | grão de `silver.bilhete_agregado` |
| `agg_viagens_periodo` | 401 | `nivel` (mes/semana/dia) × período |
| `agg_ranking_empresas` | 12.772 | `periodo_tipo` (mes/total) × período × `cnpj` |
| `agg_origem_destino` | 1.200 | `visao` × `tipo` × localidade (top 200) |
| `agg_duracao_media` | 10.821 | `nivel` (mes/empresa_mes) × período |
| `agg_qualidade_registros` | 82 | regra / origem / mês |
| `agg_evolucao_mensal_volume` | 12 | mês |

## 2. Verificação (números reais)

| Item | Resultado |
|---|---|
| (a) contagens > 0 em dimensões, fatos e agregados | sim, tabela acima |
| (b) idempotência: reexecutar não muda contagens | sim, nas duas camadas (rodadas 1 e 2 iguais para dimensões/fatos e para os 6 agregados) |
| (c) `fato_viagem` = `silver.viagem_fretado` | 76.802 = 76.802; nenhuma das 57 cópias descartadas está na fato |
| (d) chaves únicas | 0 duplicatas em `dim_empresa`, `fato_viagem`, `fato_licenca`, `dim_data`, `dim_localidade`, `dim_cobertura` |
| (e) fev/2026 | `viagens_realizadas` nulo e `status = ausente`; `viagens_licenciadas` = 25.459 (não nulo); duração média nula; MoM de mar/2026 nulo porque fev é ausente; paradas de ago/2026 `parcial` |
| (f) bilhetes | soma por `status_mes_viagem` = total da Silver = 34.634.933 |
| (g) ranking e passageiros | 0 ranks duplicados. Empresa líder: viagens 4.398 = 4.398 (Silver); passageiros 42.141 = 42.141 (Silver, 1× por licença) |
| (h) integridade de `fato_viagem` | origem e destino resolvidos em `dim_localidade`: 99,24%; CNPJ em `dim_empresa`: 100% |

**Erro encontrado e corrigido na verificação (g):** a primeira versão somava os passageiros por licença **e mês**, o que contava de novo a licença que tem viagens em vários meses (empresa líder: 43.876 contra 42.141). Passou a contar cada licença uma única vez, no mês da sua primeira viagem; a soma dos meses agora é igual ao total.

## 3. Bilhetes: mês da viagem e viés de janela (E2, G4)

Decisão: **volume transportado = mês da viagem**; **vendas = mês de emissão**, em séries separadas (`bilhetes_transportados` e `bilhetes_vendidos`).

Antecedência (mês da viagem − mês da emissão), ponderada por bilhetes: média 0,243 mês; p50 = 0; p90 = 1; **p95 = 1**; p99 = 2. Há 17.907 bilhetes com antecedência negativa (viagem antes da emissão).

Regra de `status_mes_viagem` em `fato_bilhete`:
- `incompleto_inicio`: mês da viagem anterior a `data_inicio` + p95 (1 mês), ou seja, até 2025-10;
- `confiavel`: de 2025-11 até o último mês de emissão carregado (2026-09);
- `agendado`: mês da viagem posterior a 2026-09 (fora do volume transportado).

| Status | Linhas | Bilhetes | % | Meses |
|---|---|---|---|---|
| `incompleto_inicio` | 101.979 | 2.550.296 | 7,36% | 2022-12 a 2025-10 |
| `confiavel` | 1.747.672 | 31.469.789 | 90,86% | 2025-11 a 2026-09 |
| `agendado` | | 614.848 | 1,78% | 2026-10 a 2027-07 |

Consequência: em `agg_evolucao_mensal_volume`, `bilhetes_transportados` de out/2025 é nulo (`incompleto_inicio`) e os 11 meses seguintes são confiáveis.

## 4. Respostas às 6 perguntas

### 4.1 Viagens por período (`agg_viagens_periodo`)

| Mês | Viagens realizadas (Monitriip) | Viagens licenciadas (SISAUT + contínuo) | Cobertura |
|---|---|---|---|
| 2025-10 | 8.681 | 40.610 | ok |
| 2025-11 | 8.561 | 41.996 | ok |
| 2025-12 | 7.159 | 38.133 | ok |
| 2026-01 | 6.485 | 32.308 | ok |
| 2026-02 | **nulo** | 25.459 | **ausente** |
| 2026-03 | 5.792 | 29.775 | ok |
| 2026-04 | 6.858 | 32.775 | ok |
| 2026-05 | 6.467 | 31.566 | ok |
| 2026-06 | 6.066 | 29.357 | ok |
| 2026-07 | 7.065 | 35.787 | ok |
| 2026-08 | 6.324 | 32.571 | ok |
| 2026-09 | 7.344 | 38.769 | ok |

As duas colunas são medidas diferentes: viagens licenciadas são as autorizadas; viagens realizadas são as registradas pelo Monitriip. A série de licenciadas cobre fevereiro.

### 4.2 Empresas com maior volume (`agg_ranking_empresas`, total da janela)

| # | Empresa | Viagens | Passageiros (licenças) |
|---|---|---|---|
| 1 | COOPERSIND COOPERATIVA DE TRANSPORTES DE CARGAS, PASSAGEIROS, ESCOLAR E TURISMO DE MINAS GERAIS | 4.398 | 42.141 |
| 2 | COOPERATIVA DE TRANSPORTE DE PASSAGEIROS E CARGAS, TURISMO E ESCOLAR DE MINAS GERAIS LTDA | 1.405 | 19.531 |
| 3 | COOPERATIVA DE TRANSPORTE ESCOLAR E DE PESSOAL DE MINAS GERAIS - CTESPMG | 800 | 12.211 |
| 4 | PEIXOTO TRANSPORTE E LOCACAO LTDA | 585 | 12.821 |
| 5 | AUTO VIACAO CIRCULARE POCOS DE CALDAS LTDA | 541 | 9.020 |
| 6 | BITUR TRANSPORTADORA TURISTICA LTDA | 539 | 14.184 |
| 7 | BOMPANI AGENCIA DE VIAGENS E TURISMO LTDA | 536 | 7.534 |
| 8 | MARCO ANTONIO JANUARIO & CIA LTDA | 535 | 25.772 |
| 9 | NETOS TURISMO LTDA | 518 | 11.684 |
| 10 | SERVILOC - SERVIÇOS, COMERCIO E LOCAÇÃO DE VEICULOS LTDA | 510 | 11.458 |

### 4.3 Principais origens e destinos (`agg_origem_destino`)

Visão **regular** (bilhetes por mês da viagem, status confiável, bilhetes):
- Origens: São Paulo/SP 4.295.612; Rio de Janeiro/RJ 2.365.105; Belo Horizonte/MG 1.165.158; Curitiba/PR 1.150.746; Goiânia/GO 982.282.
- Destinos: São Paulo/SP 4.225.490; Rio de Janeiro/RJ 2.324.251; Curitiba/PR 1.162.588; Belo Horizonte/MG 1.020.546; Goiânia/GO 925.704.
- Pares: São Paulo→Rio de Janeiro 982.990; Rio de Janeiro→São Paulo 961.020; São Paulo→Curitiba 339.679; Curitiba→São Paulo 314.217; Belo Horizonte→São Paulo 297.522.

Visão **fretado** (viagens realizadas, via destino principal da licença; ver seção 6):
- Origens (viagens): Belo Horizonte/MG 4.041; Juiz de Fora/MG 3.162; Brasília/DF 2.623; Rio de Janeiro/RJ 2.183; São Paulo/SP 2.105.
- Destinos (viagens): São Paulo/SP 11.227; Aparecida/SP 10.112; Rio de Janeiro/RJ 4.517; Guarapari/ES 1.672; Caldas Novas/GO 1.667; Foz do Iguaçu/PR 1.467; Curitiba/PR 1.318; Cabo Frio/RJ 1.112.
- Pares: Juiz de Fora→Rio de Janeiro 1.406; Belo Horizonte→Aparecida 657; Brasília→Goiânia 559; Belo Horizonte→Guarapari 545; Rio de Janeiro→Aparecida 518; Volta Redonda→São Paulo 512; Belo Horizonte→São Paulo 452.

### 4.4 Duração média das viagens (`agg_duracao_media`)

Exclui viagens com `duracao_atipica` e a cópia mantida dos códigos repetidos. Duração em minutos.

| Mês | Viagens consideradas | Média (min) | Mediana (min) | Cobertura |
|---|---|---|---|---|
| 2025-10 | 8.592 | 2.214,0 | 793,1 | ok |
| 2025-11 | 8.428 | 2.161,3 | 821,0 | ok |
| 2025-12 | 7.053 | 2.321,0 | 811,7 | ok |
| 2026-01 | 6.419 | 2.534,5 | 868,1 | ok |
| 2026-02 | nulo | nulo | nulo | ausente |
| 2026-03 | 5.710 | 2.353,6 | 810,0 | ok |
| 2026-04 | 6.776 | 2.386,5 | 812,5 | ok |
| 2026-05 | 6.351 | 2.243,8 | 763,2 | ok |
| 2026-06 | 5.978 | 2.420,4 | 813,2 | ok |
| 2026-07 | 6.987 | 2.401,8 | 857,8 | ok |
| 2026-08 | 6.215 | 2.262,8 | 779,2 | ok |
| 2026-09 | 7.225 | 2.122,4 | 742,0 | ok |

A média (cerca de 37 horas) é muito maior que a mediana (cerca de 13 horas): poucas viagens longas puxam a média. Para o uso corrente, a mediana é a medida mais representativa.

### 4.5 Registros duplicados ou inconsistentes (`agg_qualidade_registros`)

- 57 códigos de viagem repetidos entre arquivos: 57 cópias descartadas (quarentena) e 57 cópias mantidas na fato, com flag.
- 3 meses ausentes e 1 parcial na cobertura: viagens 2026-02, paradas 2026-02, empresas regular 2026-03 (ausentes); paradas 2026-08 (parcial).
- Demais regras (47 em 10 tabelas da Silver) e flags por mês da `fato_viagem` estão na própria tabela, por tabela, regra e mês.

### 4.6 Evolução mensal do volume (`agg_evolucao_mensal_volume`)

| Mês | Viagens realizadas | Viagens licenciadas | Passageiros (licenças) | Bilhetes transportados (mês da viagem) | Bilhetes vendidos (mês de emissão) | Viagens realizadas, MoM % | Bilhetes vendidos, MoM % |
|---|---|---|---|---|---|---|---|
| 2025-10 | 8.681 | 40.610 | 207.564 | nulo (`incompleto_inicio`) | 3.169.713 | | |
| 2025-11 | 8.561 | 41.996 | 210.087 | 2.826.083 | 3.205.078 | -1,38 | 1,12 |
| 2025-12 | 7.159 | 38.133 | 175.188 | 4.061.826 | 4.166.639 | -16,38 | 30,0 |
| 2026-01 | 6.485 | 32.308 | 153.808 | 4.000.772 | 3.633.732 | -9,41 | -12,79 |
| 2026-02 | **nulo** | 25.459 | nulo | 2.831.144 | 2.621.963 | nulo | -27,84 |
| 2026-03 | 5.792 | 29.775 | 130.986 | 2.574.761 | 2.740.287 | nulo | 4,51 |
| 2026-04 | 6.858 | 32.775 | 153.656 | 2.723.395 | 2.570.558 | 18,4 | -6,19 |
| 2026-05 | 6.467 | 31.566 | 136.068 | 2.574.073 | 2.683.307 | -5,7 | 4,39 |
| 2026-06 | 6.066 | 29.357 | 134.304 | 2.429.379 | 2.381.785 | -6,2 | -11,24 |
| 2026-07 | 7.065 | 35.787 | 161.033 | 2.946.599 | 2.979.669 | 16,47 | 25,1 |
| 2026-08 | 6.324 | 32.571 | 135.519 | 2.230.761 | 2.153.009 | -10,49 | -27,74 |
| 2026-09 | 7.344 | 38.769 | 163.266 | 2.270.996 | 2.329.193 | 16,13 | 8,18 |

A variação anual (YoY) fica nula em toda a janela, porque a janela tem 12 meses e a comparação exige o mesmo mês do ano anterior. Variação mensal de mar/2026 em viagens é nula porque fevereiro é ausente. Os bilhetes de ago e set/2026 vêm de arquivos de emissão menores que a média; a queda de 2026-08 (-27,7% nas vendas) deve ser lida com cautela.

## 5. Decisões e divergências do desenho

- **Códigos repetidos:** as 57 cópias descartadas ficam na quarentena e não entram na Gold; as 57 cópias mantidas entram em `fato_viagem` com flag e são excluídas só da duração média. O texto do desenho foi corrigido para dizer isso (antes dizia que os 57 não entravam).
- **Passageiros sem dupla contagem:** cada licença conta uma vez, no mês da primeira viagem (ver seção 2).
- **Meses de cobertura nula:** viagens realizadas e duração aparecem nulas em fev/2026 e as variações MoM/YoY só existem entre meses completos.
- **`agg_origem_destino` limita o top a 200** por visão e tipo.

## 6. Correção da regra de destino das licenças

A primeira execução dos indicadores mostrou que, na visão fretado, **origem e destino eram iguais em todos os pares** (Belo Horizonte→Belo Horizonte, etc.). A causa: `silver.licenca` definia o destino como o município final do trecho de maior sequência, e no fretamento (circuito fechado) o último trecho é o retorno à origem.

Nova regra em `04_silver_licencas`: `municipio_destino`/`uf_destino` = fim do trecho **mais longo** cujo município/UF difere da origem; se todos os trechos terminam na origem, o destino é a própria origem. O destino do último trecho foi preservado em `municipio_destino_ultimo_trecho`/`uf_destino_ultimo_trecho`. A cadeia Silver de licenças → DQ → Gold foi reexecutada (`07_rerun_destino`).

Efeito medido em `silver.licenca` (408.900 licenças SISAUT):

| Critério | Licenças com destino igual à origem |
|---|---|
| Regra antiga (destino = último trecho) | 408.856 (praticamente todas) |
| Nova regra (destino principal) | **193** |

Após o reprocessamento (`07_rerun_destino`, run `253696600786650`, SUCCESS) todas as verificações continuaram passando: fatos ok, indicadores ok (contagens iguais às anteriores: 401, 12.772, 1.200, 10.821, 82 e 12 linhas), `dq_resultados` com 47 regras em 10 tabelas, e `a_ok`, `e_ok`, `g_ok` verdadeiros.

Limites da regra: o "destino principal" é uma aproximação. Para viagens com mais de um destino intermediário, vale o trecho mais longo; viagens que ficam dentro do mesmo município têm destino igual à origem (193 licenças).

Origem e destino da visão fretado dependem do vínculo viagem ↔ licença (99,24% das viagens); as 585 viagens sem licença não entram nessa visão.
