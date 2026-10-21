CREATE OR REPLACE TABLE gold.gold_resumo_cliente_mes
USING DELTA
AS
WITH movimentacao_mensal AS (
    SELECT
        id_cliente,
        nome_cliente,
        ano_mes,
        count_if(NOT estornada) AS qtd_transacoes,
        sum(valor_liquido) AS valor_total,
        count_if(estornada) AS qtd_estornos,
        coalesce(sum(CASE WHEN estornada THEN valor END), 0) AS valor_estornado,
        count(DISTINCT CASE
            WHEN NOT estornada THEN concat_ws('|', nome_estabelecimento, cidade_estabelecimento, mcc)
        END) AS qtd_estabelecimentos_distintos
    FROM gold.gold_transacoes_cartao
    GROUP BY id_cliente, nome_cliente, ano_mes
)

SELECT
    id_cliente,
    nome_cliente,
    ano_mes,
    qtd_transacoes,
    valor_total,
    round(valor_total / nullif(qtd_transacoes, 0), 2) AS ticket_medio,
    qtd_estornos,
    valor_estornado,
    qtd_estabelecimentos_distintos
FROM movimentacao_mensal
