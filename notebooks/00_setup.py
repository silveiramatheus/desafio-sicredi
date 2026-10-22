# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup
# MAGIC Cria os schemas `bronze`, `silver`, `gold`, o Volume de landing e gera a massa fictícia diretamente no Volume.
# MAGIC Com `reset_tables = true`, as tabelas das três camadas são removidas antes, para reprocessar tudo a partir de uma massa nova.

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("start_date", "2026-10-01")
dbutils.widgets.text("days", "3")
dbutils.widgets.text("seed", "42")
dbutils.widgets.dropdown("reset_landing", "true", ["true", "false"])
dbutils.widgets.dropdown("reset_tables", "false", ["true", "false"])

# COMMAND ----------

from datetime import date

from src.config import PipelineConfig
from src.datagen.generator import generate_landing
from src.utils.spark import create_landing_volume, create_schemas, drop_schemas, get_spark

config = PipelineConfig.from_env()
spark = get_spark(config)
print(config)

# COMMAND ----------

if dbutils.widgets.get("reset_tables") == "true":
    drop_schemas(spark, config)

create_schemas(spark, config)
create_landing_volume(spark, config)
display(spark.sql(f"SHOW SCHEMAS IN {config.catalog}"))

# COMMAND ----------

if dbutils.widgets.get("reset_landing") == "true":
    dbutils.fs.rm(config.landing_path, recurse=True)

folders = generate_landing(
    config.landing_path,
    start_date=date.fromisoformat(dbutils.widgets.get("start_date")),
    days=int(dbutils.widgets.get("days")),
    seed=int(dbutils.widgets.get("seed")),
)
for folder in folders:
    print(folder)

# COMMAND ----------

display(dbutils.fs.ls(f"{config.landing_path}/{dbutils.widgets.get('start_date')}"))

# COMMAND ----------

import json

manifest_path = f"{config.landing_path}/{dbutils.widgets.get('start_date')}/_manifest.json"
with open(manifest_path, encoding="utf-8") as handle:
    print(json.dumps(json.load(handle), indent=2))
