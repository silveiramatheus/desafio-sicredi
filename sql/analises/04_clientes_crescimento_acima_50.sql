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
        LAG(valor_total) OVER (PARTITION BY id_cliente ORDER BY ano_mes) AS valor_mes_anterior
    FROM movimentacao
)

SELECT
    id_cliente,
    nome_cliente,
    mes_anterior,
    ano_mes AS mes_atual,
    valor_mes_anterior,
    valor_total AS valor_mes_atual,
    round(100 * (valor_total - valor_mes_anterior) / valor_mes_anterior, 2) AS crescimento_percentual
FROM comparativo
WHERE mes_anterior = add_months(ano_mes, -1)
  AND valor_mes_anterior > 0
  AND valor_total > valor_mes_anterior * 1.5
ORDER BY crescimento_percentual DESC, id_cliente
