# Evidências de execução

Execução no Databricks Free Edition (compute serverless), com a massa gerada pelo `notebooks/00_setup` (seed 42, cargas de 01/10, 02/10 e 03/10).

Os arquivos em `csv/` são saídas dos notebooks exportadas diretamente do Databricks e podem ser abertos no GitHub. Os arquivos `.html` são os notebooks completos, com todas as células executadas. O GitHub não renderiza esses arquivos: é preciso baixar e abrir no navegador.

## Sequência executada

Horários em UTC, extraídos do histórico Delta das tabelas.

| Horário | Etapa | Observação |
|---|---|---|
| - | `00_setup` com `reset_tables = true` | schemas recriados e massa gerada no Volume |
| 01:14:47 | `01_bronze` 01/10 | carga inicial |
| 01:15:39 | `01_bronze` 02/10 | |
| 01:16:20 | `01_bronze` 03/10 | |
| 01:18:58 | `02_silver` 01/10 | dados gravados; a validação `cliente_fora_da_versao_mais_recente` falhou (ver abaixo) |
| 01:39:42 | `02_silver` 01/10 | reexecução após a correção da validação |
| 01:41:13 | `02_silver` 02/10 | |
| 01:42:54 | `02_silver` 03/10 | |
| 01:49:12 | `01_bronze` 01/10 | reprocessamento intencional |
| 01:50:42 | `02_silver` 01/10 | reprocessamento intencional |
| - | `03_gold` e `04_analises_sql` | |

Na primeira execução da Silver, a Bronze já tinha as três datas carregadas, mas a Silver só havia processado 01/10. A validação `cliente_fora_da_versao_mais_recente` comparava a Silver com a Bronze inteira e apontou como divergentes os clientes que tinham versões mais novas em 02/10 e 03/10, ainda não processadas. Os dados estavam corretos; a regra é que estava errada. A validação passou a considerar apenas as datas já processadas pela Silver, foi incluído um teste para esse cenário em `tests/test_silver.py`, e a Silver de 01/10 foi executada novamente. Como o `MERGE` é idempotente, essa reexecução não alterou nenhum registro (versão 2 do histórico de clientes, abaixo).

## Bronze

### [Registros por data](csv/01_bronze_contagem_por_data.csv)

Quantidade de linhas gravadas na Bronze por entidade e `data_referencia`.

- 01/10 é a carga inicial, com o cadastro completo e as transações desde 01/08. Por isso tem 2604 transações, contra 54 nas cargas incrementais.
- Os números incluem os registros com problema, já que a Bronze não aplica regra de negócio. Em clientes de 01/10, por exemplo, são 165 linhas: 150 válidas e 15 que vão para a quarentena na Silver.

### [Histórico Delta de transações](csv/01_bronze_historico_transacoes.csv)

Uma versão por escrita na tabela `bronze.transacoes_cartao`.

- Versões 0, 1 e 2: cargas de 01/10, 02/10 e 03/10. Cada uma tem um `predicate` diferente em `operationParameters`, o que mostra que a escrita substituiu só a sua data.
- Versão 3: reprocessamento de 01/10. Em `operationMetrics`, `numDeletedRows = 2604` e `numOutputRows = 2604`: as linhas da carga anterior foram removidas e regravadas, sem duplicar. As cargas de 02/10 e 03/10 não foram afetadas.

## Silver

### [Histórico do MERGE de clientes](csv/02_silver_historico_merge_clientes.csv)

Operações `MERGE` na tabela `silver.clientes`, com as linhas recebidas, inseridas e atualizadas.

| Versão | Carga | Resultado |
|---|---|---|
| 1 | 01/10 | 150 clientes inseridos |
| 2 | 01/10 (reexecução) | 150 recebidos, nenhum inserido ou atualizado |
| 3 | 02/10 | 15 clientes novos inseridos e 15 atualizados |
| 4 | 03/10 | 15 clientes novos inseridos e 15 atualizados |
| 6 | 01/10 (reprocessamento após 03/10) | 150 recebidos, nenhum inserido ou atualizado |

A versão 6 é a mais importante: as versões de 01/10 chegaram depois das versões de 02/10 e 03/10 e não sobrescreveram nenhuma delas, porque o `MERGE` só atualiza quando `data_atualizacao` da origem é maior que a da tabela.

A versão 5 não aparece porque a tabela exibe apenas operações `MERGE`, e a versão 5 é uma operação de manutenção registrada pelo Delta, sem alteração de dados.

### [Validações](csv/02_silver_validacoes.csv)

Resultado de `sql/validacao_silver.sql` após a última execução. As 18 regras tiveram zero violações: chave única nas cinco tabelas, integridade referencial, ausência de valores inválidos, coerência entre datas de emissão, transação e estorno, versão mais recente de cada cliente e consistência da marcação de estornos.

## Gold

### [Alertas de qualidade](csv/03_gold_alertas_qualidade.csv)

Registros rejeitados por data, entidade e motivo, com o total lido na Bronze e o percentual.

- Dentro de cada entidade e data, todos os motivos têm a mesma quantidade (por exemplo, 31 de cada em transações de 01/10). Isso vem da massa: o gerador divide o ruído de cada arquivo igualmente entre os tipos de problema. As quantidades coincidem com o `_manifest.json` gerado junto com os arquivos, o que é verificado em `tests/test_silver.py`.
- O percentual é calculado por motivo. Em estornos, cada motivo representa 10% porque o arquivo tem 10 linhas e a taxa de ruído configurada para estornos é de 20%.
- Em 02/10 e 03/10 os números são pequenos porque os arquivos de cadastro das cargas incrementais têm poucas dezenas de linhas.

### [Ranking de estabelecimentos](csv/03_gold_ranking_top5.csv)

Os cinco estabelecimentos com maior valor transacionado em cada mês, considerando apenas transações não estornadas.

- O top 5 é definido por `ranking_valor`. A coluna `ranking_quantidade` mostra a posição do mesmo estabelecimento no ranking por quantidade, por isso pode passar de 5.
- Empates usam `DENSE_RANK`: em agosto, "Supermercado Siqueira" e "Restaurante Rodrigues" tiveram 47 transações e ficaram ambos na 4ª posição por quantidade.
- Outubro tem apenas três dias de dados, então os valores são menores que os dos outros meses.

### [Conciliação](csv/03_gold_conciliacao.csv)

A soma de `gold_resumo_cliente_mes`, a soma de `gold_ranking_estabelecimento` e a soma das transações não estornadas de `gold_transacoes_cartao` são iguais: R$ 1.016.324,87. Os R$ 10.507,44 de transações estornadas não entram em nenhum indicador. O notebook `03_gold` falha se essas somas não coincidirem.

## Análises SQL

### [Crescimento acima de 50%](csv/04_crescimento_acima_50.csv)

Clientes cujo valor movimentado cresceu mais de 50% de um mês para o mês imediatamente seguinte.

- São 26 clientes: 25 de agosto para setembro e um de setembro para outubro.
- Comparações com outubro são raras porque o mês tem apenas três dias. O cliente 149 aparece porque fez uma compra de R$ 765,97 em 01/10, valor maior que todo o seu movimento de setembro (R$ 310,73).
- O menor crescimento listado é 50,43%. Clientes com exatamente 50% não entram, porque a regra é "superior a 50%".

### [Quartis de valor movimentado](csv/04_quartis_resumo.csv)

Distribuição dos clientes em quatro grupos com `NTILE(4)`, ordenados pelo valor movimentado.

- São 145 clientes, e não 180, porque só entram clientes com pelo menos uma transação no período. Dos 35 restantes, 19 entraram nas cargas de 02/10 e 03/10 e ainda não usaram o cartão, e 16 são da carga inicial e não tiveram transações.
- Os grupos têm 37, 36, 36 e 36 clientes: quando a divisão não é exata, o `NTILE` coloca as linhas excedentes nos primeiros grupos.
- O primeiro quartil concentra R$ 524.021,13, cerca de 52% do total.
- A soma dos quatro quartis é igual ao total da conciliação (R$ 1.016.324,87).

## Notebooks completos

| Arquivo | Conteúdo |
|---|---|
| `00_setup.html` | criação dos schemas e do Volume, geração da massa e manifest de 01/10 |
| `01_bronze.html` | reprocessamento de 01/10, contagem por data e histórico Delta |
| `02_silver.html` | reprocessamento de 01/10, quarentena do dia, histórico do `MERGE`, estornos e validações |
| `03_gold.html` | as quatro tabelas Gold e a conciliação |
| `04_analises_sql.html` | resultado das cinco consultas analíticas |

O `00_setup.html` corresponde a uma execução posterior com `reset_tables = false`, feita apenas para registrar as saídas do notebook. Como a seed é fixa, os arquivos gerados são idênticos aos da execução original e as tabelas não foram alteradas.
