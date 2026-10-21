# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver
# MAGIC Tipagem, regras de qualidade, quarentena com `motivo_erro`, deduplicação por chave de negócio e `MERGE` incremental.
# MAGIC Ordem de processamento: clientes → contas → cartões → transações → estornos → marcação de transações estornadas.

# COMMAND ----------

dbutils.widgets.text("data_referencia", "2026-10-01")

# COMMAND ----------

from datetime import date

from src.config import ENTITIES, PipelineConfig
from src.transformations.silver import run_silver
from src.utils.spark import create_schemas, get_spark

config = PipelineConfig.from_env()
spark = get_spark(config)
create_schemas(spark, config)
reference_date = date.fromisoformat(dbutils.widgets.get("data_referencia"))

# COMMAND ----------

counts = run_silver(spark, config, reference_date)
display(spark.createDataFrame(
    [(entity, values["read"], values["valid"], values["rejected"]) for entity, values in counts.items()],
    "entidade string, lidos long, validos long, rejeitados long",
))

# COMMAND ----------

quarantine = " UNION ALL ".join(
    f"SELECT '{entity}' AS entidade, motivo_erro, count(*) AS registros "
    f"FROM {config.table('silver', f'quarentena_{entity}')} WHERE data_referencia = '{reference_date}' GROUP BY motivo_erro"
    for entity in ENTITIES
)
display(spark.sql(f"{quarantine} ORDER BY entidade, registros DESC"))

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {config.table('silver', 'clientes')}")
    .where("operation = 'MERGE'")
    .selectExpr(
        "version",
        "timestamp",
        "operationMetrics.numSourceRows AS linhas_origem",
        "operationMetrics.numTargetRowsInserted AS inseridos",
        "operationMetrics.numTargetRowsUpdated AS atualizados",
    )
)

# COMMAND ----------

display(spark.sql(f"""
    SELECT estornada, count(*) AS transacoes, sum(valor) AS valor_total
    FROM {config.table('silver', 'transacoes_cartao')}
    GROUP BY estornada
"""))

# COMMAND ----------

display(spark.table(config.table("silver", "quarentena_clientes")).where(f"data_referencia = '{reference_date}'"))
