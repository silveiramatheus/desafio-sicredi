WITH valor_por_cliente AS (
    SELECT
        id_cliente,
        nome_cliente,
        sum(valor_total) AS valor_movimentado,
        sum(qtd_transacoes) AS qtd_transacoes
    FROM gold.gold_resumo_cliente_mes
    GROUP BY id_cliente, nome_cliente
),

quartis AS (
    SELECT
        *,
        NTILE(4) OVER (ORDER BY valor_movimentado DESC) AS quartil
    FROM valor_por_cliente
)

SELECT
    id_cliente,
    nome_cliente,
    valor_movimentado,
    qtd_transacoes,
    quartil,
    CASE quartil
        WHEN 1 THEN 'Q1 - maior valor movimentado'
        WHEN 2 THEN 'Q2'
        WHEN 3 THEN 'Q3'
        ELSE 'Q4 - menor valor movimentado'
    END AS faixa
FROM quartis
ORDER BY quartil, valor_movimentado DESC
