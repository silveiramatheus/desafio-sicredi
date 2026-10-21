# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold
# MAGIC Tabelas analíticas recalculadas a partir da Silver (`sql/gold/*.sql`).
# MAGIC Transações estornadas permanecem na visão analítica com `valor_liquido = 0` e ficam fora de todos os indicadores financeiros.

# COMMAND ----------

from src.config import PipelineConfig
from src.transformations.gold import build_gold
from src.utils.spark import create_schemas, get_spark

config = PipelineConfig.from_env()
spark = get_spark(config)
create_schemas(spark, config)

# COMMAND ----------

counts = build_gold(spark, config)
display(spark.createDataFrame(list(counts.items()), "tabela string, registros long"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_transacoes_cartao

# COMMAND ----------

display(spark.table(config.table("gold", "gold_transacoes_cartao")).orderBy("data_transacao").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_resumo_cliente_mes

# COMMAND ----------

display(spark.table(config.table("gold", "gold_resumo_cliente_mes")).orderBy("id_cliente", "ano_mes").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_ranking_estabelecimento (top 5 por mês)

# COMMAND ----------

display(
    spark.table(config.table("gold", "gold_ranking_estabelecimento"))
    .where("ranking_valor <= 5")
    .orderBy("ano_mes", "ranking_valor")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_alertas_qualidade

# COMMAND ----------

display(spark.table(config.table("gold", "gold_alertas_qualidade")).orderBy("data_referencia", "entidade", "motivo_erro"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conciliação
# MAGIC O valor total do resumo mensal deve ser igual à soma das transações não estornadas.

# COMMAND ----------

reconciliation = spark.sql(f"""
    SELECT
        (SELECT sum(valor_total) FROM {config.table('gold', 'gold_resumo_cliente_mes')}) AS valor_resumo_mensal,
        (SELECT sum(valor_total) FROM {config.table('gold', 'gold_ranking_estabelecimento')}) AS valor_ranking_estabelecimento,
        (SELECT sum(valor) FROM {config.table('gold', 'gold_transacoes_cartao')} WHERE NOT estornada) AS valor_transacoes_nao_estornadas,
        (SELECT sum(valor) FROM {config.table('gold', 'gold_transacoes_cartao')} WHERE estornada) AS valor_estornado_excluido
""")
display(reconciliation)

# COMMAND ----------

values = reconciliation.first()
assert values["valor_resumo_mensal"] == values["valor_ranking_estabelecimento"] == values["valor_transacoes_nao_estornadas"]
