WITH versoes AS (
    SELECT
        try_cast(id_cliente AS BIGINT) AS id_cliente,
        nome,
        cidade,
        estado,
        try_to_timestamp(data_atualizacao, 'yyyy-MM-dd HH:mm:ss') AS data_atualizacao,
        data_referencia,
        timestamp_ingestao
    FROM bronze.clientes
),

versoes_ordenadas AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id_cliente
            ORDER BY data_atualizacao DESC, timestamp_ingestao DESC
        ) AS ordem_versao,
        COUNT(*) OVER (PARTITION BY id_cliente) AS qtd_versoes
    FROM versoes
    WHERE id_cliente IS NOT NULL
      AND data_atualizacao IS NOT NULL
)

SELECT
    id_cliente,
    nome,
    cidade,
    estado,
    data_atualizacao,
    data_referencia,
    qtd_versoes
FROM versoes_ordenadas
WHERE ordem_versao = 1
  AND qtd_versoes > 1
ORDER BY qtd_versoes DESC, id_cliente
