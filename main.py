import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import * 
from common_lib import validate_and_load_configuration
from common_lib import apply_normalization
from common_lib import check_and_enforce_schema

# Spark session & context
spark = SparkSession.builder.master('local').getOrCreate()

# Load and validate configuration file 
config = validate_and_load_configuration()

'''
 -- Pre-Processing -- 
'''
src_df = spark.read \
    .option("escape", "\\") \
    .json("source/supplier_car.json", encoding='UTF-8')

# Pivot to Achieve desired granularity 
pre_df = src_df.groupBy("ID", "MakeText", "ModelText", "ModelTypeText", "TypeName", "TypeNameFull") \
    .pivot("Attribute Names") \
    .agg(first("Attribute Values")) 

# Preprocess OUTPUT
pre_df.write.mode("overwrite") \
    .option("header", "true") \
    .csv("csv/preprocessing", encoding='UTF-8')


''' 
-- Normalization Step --
 This step is configurable through normalization.json file. You may add new column mappings there if needed.
'''
normalized_df = apply_normalization(
    pre_df,
    config.get("to_normalize"),
    config.get("mappings")
)

# Normalization OUTPUT
normalized_df.write.mode("overwrite") \
    .option("header", "true") \
    .option("encoding", "UTF-8") \
    .csv("csv/normalization")


'''
-- Extraction Step --
'''
extracted_df = normalized_df \
    .withColumn("value-ConsumptionTotalText", split(normalized_df["ConsumptionTotalText"], " ")[0]) \
    .withColumn("unit-ConsumptionTotalText", split(normalized_df["ConsumptionTotalText"], " ")[1]) \
    .drop("ConsumptionTotalText")

# Extraction OUTPUT
extracted_df.write.mode("overwrite") \
    .option("header", "true") \
    .option("encoding", "UTF-8") \
    .csv("csv/extraction")


'''
-- Integration Step --
'''
# Load Integration Rules 
with open("configuration/integration.json", "r") as f:
    integration_config = json.load(f)

# Drop columns not in Sink/Destination. Columns are defined in integration configuration file  
integration_df = extracted_df.drop(*integration_config.get("to_drop"))

# Rename Columns to Match Destination
for key, value in integration_config.get("to_rename").items():
    integration_df = integration_df.withColumnRenamed(key, value)

# Enrich Dataframe with Missing Columns 
for key, value in integration_config.get("to_add").items():
    integration_df = integration_df.withColumn(key, lit(value))

integration_df = check_and_enforce_schema(integration_df, "target")
integration_df.show()

# Integration OUTPUT
integration_df.write.mode("overwrite") \
    .option("header", "true") \
    .option("encoding", "UTF-8") \
    .csv("csv/integration")

# Check and enforce schema
integration_df = check_and_enforce_schema(integration_df, "target")

