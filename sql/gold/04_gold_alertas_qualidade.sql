CREATE OR REPLACE TABLE gold.gold_alertas_qualidade
USING DELTA
AS
WITH rejeitados AS (
    SELECT 'clientes' AS entidade, data_referencia, motivo_erro FROM silver.quarentena_clientes
    UNION ALL
    SELECT 'contas', data_referencia, motivo_erro FROM silver.quarentena_contas
    UNION ALL
    SELECT 'cartoes', data_referencia, motivo_erro FROM silver.quarentena_cartoes
    UNION ALL
    SELECT 'transacoes_cartao', data_referencia, motivo_erro FROM silver.quarentena_transacoes_cartao
    UNION ALL
    SELECT 'estornos', data_referencia, motivo_erro FROM silver.quarentena_estornos
),

motivos AS (
    SELECT entidade, data_referencia, explode(split(motivo_erro, ';')) AS motivo_erro
    FROM rejeitados
),

lidos_bronze AS (
    SELECT 'clientes' AS entidade, data_referencia, count(*) AS qtd_lidos_bronze FROM bronze.clientes GROUP BY data_referencia
    UNION ALL
    SELECT 'contas', data_referencia, count(*) FROM bronze.contas GROUP BY data_referencia
    UNION ALL
    SELECT 'cartoes', data_referencia, count(*) FROM bronze.cartoes GROUP BY data_referencia
    UNION ALL
    SELECT 'transacoes_cartao', data_referencia, count(*) FROM bronze.transacoes_cartao GROUP BY data_referencia
    UNION ALL
    SELECT 'estornos', data_referencia, count(*) FROM bronze.estornos GROUP BY data_referencia
)

SELECT
    m.data_referencia,
    m.entidade,
    m.motivo_erro,
    count(*) AS qtd_registros,
    l.qtd_lidos_bronze,
    round(100 * count(*) / l.qtd_lidos_bronze, 2) AS percentual_rejeitado
FROM motivos AS m
JOIN lidos_bronze AS l
    ON l.entidade = m.entidade
   AND l.data_referencia = m.data_referencia
GROUP BY m.data_referencia, m.entidade, m.motivo_erro, l.qtd_lidos_bronze
