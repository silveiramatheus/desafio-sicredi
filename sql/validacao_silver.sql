WITH clientes_validos_bronze AS (
    SELECT b.*
    FROM bronze.clientes AS b
    LEFT ANTI JOIN silver.quarentena_clientes AS q
        ON b.hash_linha = q.hash_linha
       AND q.motivo_erro NOT IN ('REGISTRO_DUPLICADO', 'VERSAO_DESATUALIZADA')
),

versao_mais_recente AS (
    SELECT
        try_cast(id_cliente AS BIGINT) AS id_cliente,
        trim(cidade) AS cidade,
        try_to_timestamp(data_atualizacao, 'yyyy-MM-dd HH:mm:ss') AS data_atualizacao,
        ROW_NUMBER() OVER (
            PARTITION BY id_cliente
            ORDER BY try_to_timestamp(data_atualizacao, 'yyyy-MM-dd HH:mm:ss') DESC
        ) AS ordem
    FROM clientes_validos_bronze
),

checagens AS (
    SELECT 'chave_unica_clientes' AS regra, count(*) - count(DISTINCT id_cliente) AS violacoes
    FROM silver.clientes

    UNION ALL
    SELECT 'chave_unica_contas', count(*) - count(DISTINCT id_conta)
    FROM silver.contas

    UNION ALL
    SELECT 'chave_unica_cartoes', count(*) - count(DISTINCT id_cartao)
    FROM silver.cartoes

    UNION ALL
    SELECT 'chave_unica_transacoes', count(*) - count(DISTINCT id_transacao)
    FROM silver.transacoes_cartao

    UNION ALL
    SELECT 'chave_unica_estornos', count(*) - count(DISTINCT id_estorno)
    FROM silver.estornos

    UNION ALL
    SELECT 'conta_sem_cliente', count(*)
    FROM silver.contas AS c
    LEFT ANTI JOIN silver.clientes AS p ON c.id_cliente = p.id_cliente

    UNION ALL
    SELECT 'cartao_sem_conta', count(*)
    FROM silver.cartoes AS c
    LEFT ANTI JOIN silver.contas AS p ON c.id_conta = p.id_conta

    UNION ALL
    SELECT 'transacao_sem_cartao', count(*)
    FROM silver.transacoes_cartao AS c
    LEFT ANTI JOIN silver.cartoes AS p ON c.id_cartao = p.id_cartao

    UNION ALL
    SELECT 'estorno_sem_transacao', count(*)
    FROM silver.estornos AS c
    LEFT ANTI JOIN silver.transacoes_cartao AS p ON c.id_transacao = p.id_transacao

    UNION ALL
    SELECT 'valor_negativo_ou_nulo', count(*)
    FROM silver.transacoes_cartao
    WHERE valor < 0 OR valor IS NULL

    UNION ALL
    SELECT 'data_transacao_nula', count(*)
    FROM silver.transacoes_cartao
    WHERE data_transacao IS NULL

    UNION ALL
    SELECT 'status_conta_fora_do_dominio', count(*)
    FROM silver.contas
    WHERE status_conta NOT IN ('ATIVA', 'BLOQUEADA', 'ENCERRADA')

    UNION ALL
    SELECT 'status_cartao_fora_do_dominio', count(*)
    FROM silver.cartoes
    WHERE status_cartao NOT IN ('ATIVO', 'BLOQUEADO', 'CANCELADO')

    UNION ALL
    SELECT 'cliente_fora_da_versao_mais_recente', count(*)
    FROM versao_mais_recente AS v
    JOIN silver.clientes AS s ON s.id_cliente = v.id_cliente
    WHERE v.ordem = 1
      AND (v.cidade <> s.cidade OR v.data_atualizacao <> s.data_atualizacao)

    UNION ALL
    SELECT 'estorno_sem_marcacao', count(*)
    FROM silver.estornos AS e
    JOIN silver.transacoes_cartao AS t ON t.id_transacao = e.id_transacao
    WHERE NOT t.estornada

    UNION ALL
    SELECT 'marcacao_sem_estorno', count(*)
    FROM silver.transacoes_cartao AS t
    LEFT ANTI JOIN silver.estornos AS e ON e.id_transacao = t.id_transacao
    WHERE t.estornada
)

SELECT
    regra,
    violacoes,
    CASE WHEN violacoes = 0 THEN 'OK' ELSE 'FALHA' END AS status
FROM checagens
