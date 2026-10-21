CREATE OR REPLACE TABLE gold.gold_transacoes_cartao
USING DELTA
AS
SELECT
    t.id_transacao,
    t.data_transacao,
    CAST(date_trunc('MONTH', t.data_transacao) AS DATE) AS ano_mes,
    t.valor,
    t.estornada,
    CASE WHEN t.estornada THEN CAST(0 AS DECIMAL(18, 2)) ELSE t.valor END AS valor_liquido,
    t.nome_estabelecimento,
    t.cidade_estabelecimento,
    t.mcc,
    t.id_cartao,
    ca.bandeira,
    ca.tipo_cartao,
    ca.status_cartao,
    co.id_conta,
    co.tipo_conta,
    co.status_conta,
    cl.id_cliente,
    cl.nome AS nome_cliente,
    cl.cidade AS cidade_cliente,
    cl.estado AS estado_cliente,
    t.data_referencia
FROM silver.transacoes_cartao AS t
JOIN silver.cartoes AS ca ON ca.id_cartao = t.id_cartao
JOIN silver.contas AS co ON co.id_conta = ca.id_conta
JOIN silver.clientes AS cl ON cl.id_cliente = co.id_cliente
