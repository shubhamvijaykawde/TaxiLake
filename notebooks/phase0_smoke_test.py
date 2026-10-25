# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 0 smoke test
# MAGIC Prints runtime versions, writes a tiny managed Delta table to `<catalog>.ops.smoke_test`,
# MAGIC reads it back and shows the Delta transaction history. Requires `sql/00_setup_catalog.sql` first.

# COMMAND ----------

import sys

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

dbutils.widgets.text("catalog", "workspace")  # noqa: F821 - dbutils is provided by Databricks
catalog = dbutils.widgets.get("catalog")  # noqa: F821
table = f"{catalog}.ops.smoke_test"

print(f"Python: {sys.version}")
print(f"Spark:  {spark.version}")  # noqa: F821 - spark is provided by Databricks
print(f"Target: {table}")

# COMMAND ----------

schema = StructType(
    [
        StructField("id", IntegerType(), nullable=False),
        StructField("label", StringType(), nullable=False),
    ]
)
rows = [(1, "bronze"), (2, "silver"), (3, "gold")]

df = spark.createDataFrame(rows, schema)  # noqa: F821
df.write.format("delta").mode("overwrite").saveAsTable(table)

# COMMAND ----------

result = spark.table(table)  # noqa: F821
count = result.count()
print(f"Rows read back: {count}")
assert count == len(rows), f"Expected {len(rows)} rows, found {count}"
display(result.orderBy("id"))  # noqa: F821

# COMMAND ----------

# Each overwrite adds a new version to the Delta log; this is what time travel reads from.
display(spark.sql(f"DESCRIBE HISTORY {table}").select("version", "timestamp", "operation"))  # noqa: F821
