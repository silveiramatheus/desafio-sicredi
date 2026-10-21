CREATE OR REPLACE TABLE gold.gold_ranking_estabelecimento
USING DELTA
AS
WITH movimentacao_estabelecimento AS (
    SELECT
        ano_mes,
        nome_estabelecimento,
        cidade_estabelecimento,
        mcc,
        count(*) AS qtd_transacoes,
        sum(valor) AS valor_total,
        count(DISTINCT id_cliente) AS qtd_clientes_distintos
    FROM gold.gold_transacoes_cartao
    WHERE NOT estornada
    GROUP BY ano_mes, nome_estabelecimento, cidade_estabelecimento, mcc
)

SELECT
    ano_mes,
    nome_estabelecimento,
    cidade_estabelecimento,
    mcc,
    qtd_transacoes,
    valor_total,
    round(valor_total / qtd_transacoes, 2) AS ticket_medio,
    qtd_clientes_distintos,
    DENSE_RANK() OVER (PARTITION BY ano_mes ORDER BY valor_total DESC) AS ranking_valor,
    DENSE_RANK() OVER (PARTITION BY ano_mes ORDER BY qtd_transacoes DESC) AS ranking_quantidade
FROM movimentacao_estabelecimento
