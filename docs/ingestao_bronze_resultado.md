# Resultado da ingestão e da camada Bronze

Janela: out/2025 a set/2026. Execução no Databricks Free (serverless), catálogo `workspace`.
Notebooks no workspace: `/Users/uilian.senna@gmail.com/antt-lakehouse/` (`01_setup`, `02_ingest_download`, `03_bronze`, `99_verificacao_bronze`, `98_diag_sisaut`).

## 1. Arquivos baixados por fonte (Volume `/Volumes/workspace/antt_raw/landing/<fonte>/`)

| Fonte | Arquivos | Bytes | Observação |
|---|---|---|---|
| viagens_fretado | 11 | 12.545.620 | fev/2026 não existe no CKAN |
| paradas_fretado | 11 | 14.469.225 | fev/2026 não existe no CKAN |
| bilhetes | 12 | 259.217.355 | |
| empresas_fretamento | 12 | 7.987.300 | |
| empresas_regular | 11 | 310.797 | mar/2026 não existe no CKAN |
| licencas_continuo | 2 | 5.632.279 | anos 2025 e 2026 |
| sisaut | 12 | 434.426.819 | |
| **Total** | **71** | **734.589.395** | 0 falhas de download |

Nenhum download falhou. A retentativa (4 tentativas, backoff de 3 s dobrando) não precisou ser acionada. Uma segunda execução do `02_ingest_download` pulou os 71 arquivos (manifesto com o mesmo `last_modified`) e baixou 0 bytes.

Arquivos fora da janela (sobras do notebook de probe) permanecem no Volume e são ignorados pela Bronze: `sisaut/...-out2026.csv`, a pasta `empresas/` e `perfil_fontes.json`.

## 2. Linhas por tabela Bronze (`workspace.antt_bronze`)

| Tabela | Linhas | Arquivos | Colunas de negócio |
|---|---|---|---|
| bronze_viagens_fretado | 76.859 | 11 | 13 |
| bronze_paradas_fretado | 102.884 | 11 | 11 |
| bronze_bilhetes | 1.932.894 | 12 | 9 |
| bronze_empresas | 118.372 (fretamento 113.745, regular 4.627) | 23 | 6 (inclui `numero_taf`, `numero_tar`, `tipo_empresa`) |
| bronze_licencas_continuo | 17.421 | 2 | 21 |
| bronze_sisaut | 1.108.375 | 12 | 30 |

Cada tabela tem também `_row_hash`, `_source_file`, `_file_mod_time`, `_ingest_ts`, `_fonte`.

## 3. Linhas por mês

| Mês | viagens | paradas | bilhetes | empresas (fret.+reg.) | sisaut |
|---|---|---|---|---|---|
| 2025-10 | 8.692 | 19.991 | 178.493 | 9.764 | 108.143 |
| 2025-11 | 8.562 | 20.712 | 168.401 | 9.802 | 113.074 |
| 2025-12 | 7.160 | 11.835 | 180.933 | 9.812 | 101.222 |
| 2026-01 | 6.494 | 16.238 | 179.796 | 9.768 | 91.740 |
| 2026-02 | sem arquivo | sem arquivo | 157.555 | 9.857 | 67.795 |
| 2026-03 | 5.793 | 6.853 | 164.417 | 9.409 (só fretamento) | 79.524 |
| 2026-04 | 6.877 | 6.187 | 159.105 | 9.912 | 88.420 |
| 2026-05 | 6.472 | 2.919 | 167.380 | 9.937 | 84.111 |
| 2026-06 | 6.068 | 7.358 | 158.973 | 9.970 | 81.184 |
| 2026-07 | 7.072 | 5.162 | 156.453 | 10.011 | 100.509 |
| 2026-08 | 6.325 | 180 | 128.972 | 10.045 | 87.295 |
| 2026-09 | 7.344 | 5.449 | 132.416 | 10.085 | 105.358 |

Licenças contínuo (anual): 2025 = 8.867 linhas, 2026 = 8.554 linhas.

Pontos de atenção nos volumes:
- `parada_fretado_08_2026.csv` tem só 180 linhas (25 KB), contra 2.919 a 20.712 nos outros meses. Parece publicação incompleta na origem.
- `sisaut` fev/2026 (67.795) é o menor mês, mas o arquivo vem íntegro (26,6 MB).
- Bilhetes: ago e set/2026 são menores que os meses anteriores (128.972 e 132.416 contra 156.000 a 180.000).

## 4. Colunas divergentes entre meses

Não há layout antigo vs novo dentro da janela. Em cada fonte, todos os arquivos têm as mesmas colunas, na mesma ordem. Colunas após a normalização do cabeçalho:
- `sisaut`: `uf_município_origem` virou `uf_municipio_origem`; `UF_inicio_trecho` virou `uf_inicio_trecho`; `UF_final_trecho` virou `uf_final_trecho`.
- `empresas`: o layout muda entre os tipos, não entre meses. Fretamento tem `numero_taf` e regular tem `numero_tar`. Na tabela, as duas colunas existem e ficam nulas para o outro tipo (`tipo_empresa` identifica a origem). Os 11 arquivos regulares aparecem como "divergentes" do layout de referência (fretamento) por esse motivo.

O `_rescued_data` do desenho original não se aplica: a carga é arquivo a arquivo com `unionByName` explícito, que cobriria colunas novas com `mergeSchema`.

## 5. Encoding por arquivo

Detecção lendo o arquivo inteiro (utf-8 se decodifica por completo, senão latin1).

| Fonte | Encoding | Arquivos fora do padrão |
|---|---|---|
| viagens_fretado | latin1 | nenhum |
| paradas_fretado | utf-8 | nenhum |
| bilhetes | latin1 | nenhum |
| empresas (fretamento e regular) | latin1 | nenhum |
| licencas_continuo | utf-8 | nenhum |
| sisaut | latin1 | nenhum |

Nenhuma fonte variou de encoding entre meses na janela.

### Texto corrompido na origem (SISAUT)

26.905 linhas do `bronze_sisaut` (2,4%) têm texto corrompido, distribuídas em todos os 12 meses (1.531 a 2.806 por mês). As colunas afetadas são `contratante` (16.796 linhas), `localidade_inicio_trecho` (10.483) e `localidade_final_trecho` (248). Exemplos: `NITERÃI`, `LOCAÃ§Ã£O`, `SERVIÃOS`. A causa está no arquivo da ANTT: algumas linhas foram gravadas em UTF-8 dentro de um arquivo latin1, e algumas têm dupla codificação (`LOCAÃÆÃ¢Â¬Â¡AO`). Como o arquivo inteiro não decodifica como UTF-8, a detecção por arquivo escolhe latin1 e as linhas UTF-8 aparecem como mojibake. A Bronze preserva o valor como veio. A correção (tentar `encode('latin1').decode('utf-8')` por valor) fica para a Silver. Nenhuma outra tabela tem esse problema (0 linhas nas outras cinco).

## 6. Verificação

Rodada pelo notebook `99_verificacao_bronze`, submetido como job, antes e depois de uma segunda execução do `03_bronze`.

- Contagens > 0 nas 6 tabelas: sim (valores da seção 2).
- Idempotência: a segunda execução do `03_bronze` carregou 0 arquivos novos, e as 6 contagens ficaram idênticas (76.859 / 102.884 / 1.932.894 / 118.372 / 17.421 / 1.108.375).
- Acentuação em `bilhetes.ponto_origem_viagem`: `Alagoinhas/BA` (2.603 linhas) e `Macaé/RJ` (4.373 linhas) corretos. Outros exemplos: `Axixá do Tocantins/TO`, `São João del Rei/MG`, `Conceição da Barra/ES`.
- `viagens_fretado.in_transbordo`: `NÃO` 74.395 e `SIM` 2.464, acento correto.
- Padrão de texto corrompido (`U+FFFD`, `Ã`/`Â` seguido de byte 0x80-0xBF) em todas as colunas de negócio: 0 linhas em viagens, paradas, bilhetes, empresas e licenças contínuo. 26.905 no sisaut (seção 5).
- Manifesto `antt_raw.ingest_manifest`: todos os registros com `status = ok` (viagens 11, paradas 11, bilhetes 12, fretamento 23, regular 21, contínuo 2, sisaut 12).

## 7. Falhas e desvios do desenho

1. O CKAN não publica fev/2026 de viagens e paradas, nem mar/2026 de empresas regular. O notebook segue sem eles e não registra erro, porque o recurso não existe.
2. `empresas-habilitadas` publica vários recursos CSV para o mesmo mês e o mesmo nome de arquivo (snapshots de datas diferentes). A primeira execução baixou 23 arquivos de fretamento e 21 de regular para 12 e 11 nomes (as cópias sobrescreveram umas às outras). Corrigi o `02_ingest_download` para ficar, por nome de arquivo, com o recurso de `last_modified` mais recente e para pular com base no último download daquele nome. Na segunda execução 11 recursos de fretamento e 10 de regular foram descartados como duplicados, e nada foi rebaixado. O manifesto mantém os 44 registros históricos desses downloads.
3. Em vez de Auto Loader (seção 7 do desenho), a Bronze usa leitura arquivo a arquivo com `spark.read` e append controlado por `_source_file`, como pedido, porque o encoding varia por arquivo na origem.
4. Bytes totais baixados na primeira execução: 742.125.230 (inclui as cópias duplicadas de empresas). O Volume mantém 734,6 MB nos 71 arquivos da janela.
5. A coluna `tipo_empresa` e a ausência de `_rescued_data` são adições em relação ao desenho.

## 8. Próximos passos

1. Silver: reparar mojibake do SISAUT em `contratante` e `localidade_*`; tratar `numero_taf`/`numero_tar` em `empresa`; normalizar CNPJ, datas e decimais conforme a seção 8 do desenho.
2. Decidir o tratamento de `parada_fretado_08_2026.csv` (180 linhas) e dos meses ausentes (viagens/paradas fev/2026, empresas regular mar/2026). Confirmar com a ANTT ou aceitar o buraco e sinalizar na Gold.
3. Empresas: guardar o snapshot por mês (já está em `_source_file`) e usar o mais recente por CNPJ na dimensão.
4. Agendar `02_ingest_download` e `03_bronze` mensalmente. A idempotência já está validada. Arquivo republicado com `last_modified` novo é rebaixado, mas a Bronze não recarrega um `_source_file` já existente. Se isso for necessário, incluir `_file_mod_time` na chave de controle.
5. Parar de manter arquivos fora da janela no Volume (`sisaut ...out2026.csv`, pasta `empresas/`) quando a janela virar parâmetro de produção.
