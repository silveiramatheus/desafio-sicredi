WITH valor_por_cliente AS (
    SELECT
        estado_cliente,
        id_cliente,
        nome_cliente,
        count(*) AS qtd_transacoes,
        sum(valor) AS valor_total
    FROM gold.gold_transacoes_cartao
    WHERE NOT estornada
    GROUP BY estado_cliente, id_cliente, nome_cliente
),

ranking AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY estado_cliente ORDER BY valor_total DESC) AS ranking_estado,
        DENSE_RANK() OVER (ORDER BY valor_total DESC) AS ranking_geral
    FROM valor_por_cliente
)

SELECT
    estado_cliente,
    ranking_estado,
    id_cliente,
    nome_cliente,
    qtd_transacoes,
    valor_total,
    ranking_geral
FROM ranking
WHERE ranking_estado <= 3
ORDER BY estado_cliente, ranking_estado
