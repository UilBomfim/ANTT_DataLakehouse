# Minuta de pedido à ANTT (Fala.BR, Lei de Acesso à Informação)

Rascunho para envio. Revise, preencha os dados do solicitante e envie pela plataforma Fala.BR (órgão: Agência Nacional de Transportes Terrestres).

**Assunto:** Dados do Monitriip (serviço fretado) ausentes ou incompletos no Portal de Dados Abertos

**Texto do pedido:**

Ao consultar o Portal de Dados Abertos da ANTT (dados.antt.gov.br), identifiquei lacunas em conjuntos do Monitriip do serviço fretado. Solicito a disponibilização dos arquivos abaixo ou, se não existirem, a informação sobre o motivo da ausência.

1. **Monitriip Serviço Fretado - Viagens**, referência fevereiro/2026. O conjunto tem os arquivos mensais de janeiro/2026 (publicado em 15/02/2026) e de março/2026 (publicado em 15/04/2026), mas não tem o de fevereiro/2026. O histórico de atividade do conjunto não registra publicação em 15/03/2026.
2. **Monitriip Serviço Fretado - Paradas**, referência fevereiro/2026, com a mesma ausência.
3. **Monitriip Serviço Fretado - Paradas**, referência agosto/2026. O arquivo `parada_fretado_08_2026.csv` (publicado em 15/09/2026) tem 25 KB e 180 linhas, contra 2.919 a 20.712 linhas nos demais meses de out/2025 a set/2026. Os arquivos de paradas de set/2026 não contêm paradas de agosto. Solicito a republicação do arquivo completo.
4. **Empresas Habilitadas**, referência março/2026, modalidade Regular. Só existe o arquivo em formato JSON, sem o CSV que existe para os demais meses. O JSON de março tem o mesmo tamanho do de fevereiro (72.322 bytes). O JSON de fretamento de março aponta para um arquivo nomeado `empresas_habilitadas_fretamento_02_2026.json`.
5. **Monitriip Serviço Fretado - Viagens**: 57 valores de `codigo_viagem` aparecem em dois arquivos mensais diferentes, com datas de início e fim distintas, mantendo o mesmo CNPJ e a mesma placa. Solicito esclarecimento se o código pode ser reutilizado ou se corresponde a um encadeamento de viagens (por exemplo, transbordo), para que a chave seja usada corretamente.

Finalidade: análise acadêmica e técnica da operação do transporte rodoviário interestadual de passageiros com dados abertos.

Formato desejado: os mesmos arquivos CSV, com o mesmo layout dos meses publicados.

---

**Evidências (anexar se a plataforma permitir):**
- Lista de recursos do CKAN dos conjuntos `monitriip-servico-fretado-viagens`, `monitriip-servico-fretado-paradas` e `empresas-habilitadas`, obtida em 03/11/2026 (`data/recursos_antt.csv` do projeto).
- Contagem de linhas por arquivo (`docs/ingestao_bronze_resultado.md`, seção 3).
