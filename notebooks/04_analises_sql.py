# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Análises SQL
# MAGIC Consultas do item 6 do desafio (`sql/analises/*.sql`), executadas sobre as camadas Bronze e Gold.

# COMMAND ----------

from src.config import PROJECT_ROOT, PipelineConfig
from src.utils.spark import get_spark, run_sql_file

config = PipelineConfig.from_env()
spark = get_spark(config)
ANALYSES = PROJECT_ROOT / "sql" / "analises"


def run_analysis(file_name: str):
    return run_sql_file(spark, config, ANALYSES / file_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Versão mais recente do cliente (CTE + ROW_NUMBER)
# MAGIC Clientes com mais de uma versão na Bronze e a versão vigente, escolhida por `data_atualizacao`.

# COMMAND ----------

display(run_analysis("01_versao_mais_recente_cliente.sql"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Variação mensal por cliente (LAG + LEAD)

# COMMAND ----------

display(run_analysis("02_variacao_mensal_cliente.sql"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ranking de clientes por estado (RANK + DENSE_RANK)
# MAGIC `RANK` mantém o salto após empates dentro do estado; `DENSE_RANK` gera o ranking geral sem saltos.

# COMMAND ----------

display(run_analysis("03_ranking_clientes_por_estado.sql"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Clientes com crescimento superior a 50% entre meses consecutivos

# COMMAND ----------

display(run_analysis("04_clientes_crescimento_acima_50.sql"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Quartis de valor movimentado (NTILE)

# COMMAND ----------

quartiles = run_analysis("05_quartis_valor_movimentado.sql")
display(quartiles)

# COMMAND ----------

display(
    quartiles.groupBy("quartil", "faixa")
    .agg({"id_cliente": "count", "valor_movimentado": "sum"})
    .withColumnRenamed("count(id_cliente)", "qtd_clientes")
    .withColumnRenamed("sum(valor_movimentado)", "valor_movimentado")
    .orderBy("quartil")
)
