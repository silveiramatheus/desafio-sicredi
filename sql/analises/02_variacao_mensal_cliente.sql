WITH movimentacao AS (
    SELECT
        id_cliente,
        nome_cliente,
        ano_mes,
        valor_total
    FROM gold.gold_resumo_cliente_mes
),

comparativo AS (
    SELECT
        id_cliente,
        nome_cliente,
        ano_mes,
        valor_total,
        LAG(ano_mes) OVER (PARTITION BY id_cliente ORDER BY ano_mes) AS mes_anterior,
        LAG(valor_total) OVER (PARTITION BY id_cliente ORDER BY ano_mes) AS valor_mes_anterior,
        LEAD(valor_total) OVER (PARTITION BY id_cliente ORDER BY ano_mes) AS valor_proximo_mes
    FROM movimentacao
)

SELECT
    id_cliente,
    nome_cliente,
    ano_mes,
    valor_total,
    mes_anterior,
    valor_mes_anterior,
    valor_total - valor_mes_anterior AS variacao_absoluta,
    round(100 * (valor_total - valor_mes_anterior) / nullif(valor_mes_anterior, 0), 2) AS variacao_percentual,
    valor_proximo_mes
FROM comparativo
ORDER BY id_cliente, ano_mes
