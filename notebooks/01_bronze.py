# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze
# MAGIC Ingestão dos arquivos de landing de uma `data_referencia` em tabelas Delta, sem regras de negócio.
# MAGIC Reprocessar a mesma data substitui apenas aquela carga (`replaceWhere`), e o histórico Delta preserva as versões anteriores.

# COMMAND ----------

dbutils.widgets.text("data_referencia", "2026-10-01")

# COMMAND ----------

from datetime import date

from src.config import ENTITIES, PipelineConfig
from src.ingestion.bronze import ingest_reference_date
from src.utils.spark import create_schemas, get_spark

config = PipelineConfig.from_env()
spark = get_spark(config)
create_schemas(spark, config)
reference_date = date.fromisoformat(dbutils.widgets.get("data_referencia"))

# COMMAND ----------

counts = ingest_reference_date(spark, config, reference_date)
display(spark.createDataFrame(list(counts.items()), "entidade string, registros long"))

# COMMAND ----------

summary = " UNION ALL ".join(
    f"SELECT '{entity}' AS entidade, data_referencia, count(*) AS registros FROM {config.table('bronze', entity)} GROUP BY data_referencia"
    for entity in ENTITIES
)
display(spark.sql(f"{summary} ORDER BY entidade, data_referencia"))

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {config.table('bronze', 'transacoes_cartao')}")
    .select("version", "timestamp", "operation", "operationParameters", "operationMetrics")
)

# COMMAND ----------

display(spark.table(config.table("bronze", "clientes")).where(f"data_referencia = '{reference_date}'").limit(10))
