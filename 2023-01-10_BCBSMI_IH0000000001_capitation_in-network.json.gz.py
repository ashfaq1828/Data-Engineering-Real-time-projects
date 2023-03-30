# Databricks notebook source
df_9_68_MB = spark.read.text("dbfs:/mnt/jan2023/2023-01-10_BCBSMI_IH0000000001_capitation_in-network.json.gz")

# COMMAND ----------

df_9_68_MB.write.mode("overwrite").text("/mnt/jan2023/capitation/2023-01-10_BCBSMI_IH0000000001_capitation_in-network/")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Reading the text file which is in 2023-01-10_BCBSMI_IH0000000001_capitation_in-network folder in jan2023 container

# COMMAND ----------

df_text_json = spark.read.json("/mnt/jan2023/capitation/2023-01-10_BCBSMI_IH0000000001_capitation_in-network/")

# COMMAND ----------

df_text_json.printSchema()

# COMMAND ----------

df_root=df_text_json.select(df_text_json.last_updated_on,df_text_json.reporting_entity_name,
                            df_text_json.reporting_entity_type,df_text_json.version)

df_root.dropna().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Loading columns of "In_network" Flatten

# COMMAND ----------

df_in_network = df_text_json.select(df_text_json.in_network)
df_in_network.printSchema()

# COMMAND ----------

from pyspark.sql.functions import explode, col

df_in_network_main = df_in_network.withColumn("in_network", explode("in_network"))\
        .withColumn("billing_code", col("in_network.billing_code"))\
        .withColumn("billing_code_type", col("in_network.billing_code_type"))\
        .withColumn("billing_code_type_version", col("in_network.billing_code_type_version"))\
        .withColumn("covered_services",explode("in_network.covered_services"))\
        .withColumn("b_billing_code",col("covered_services.billing_code"))\
        .withColumn("b_billing_code_type",col("covered_services.billing_code_type"))\
        .withColumn("b_billing_code_type_version",col("covered_services.billing_code_type_version"))\
        .withColumn("b_description",col("covered_services.description"))\
        .withColumn("description", col("in_network.description"))\
        .withColumn("name", col("in_network.name"))\
        .withColumn("negotiated_rates", explode("in_network.negotiated_rates"))\
        .withColumn("negotiated_prices", explode("negotiated_rates.negotiated_prices"))\
        .withColumn( "additional_information", col("negotiated_prices.additional_information"))\
        .withColumn("billing_class", col("negotiated_prices.billing_class"))\
        .withColumn("billing_code_modifier", explode("negotiated_prices.billing_code_modifier"))\
        .withColumn("expiration_date",col("negotiated_prices.expiration_date"))\
        .withColumn("negotiated_rate", col("negotiated_prices.negotiated_rate"))\
        .withColumn("negotiated_type", col("negotiated_prices.negotiated_type"))\
        .withColumn("provider_groups",explode("negotiated_rates.provider_groups"))\
        .withColumn("npi",explode("provider_groups.npi"))\
        .withColumn("tin", col("provider_groups.tin.type"))\
        .withColumn("tin_val",col("provider_groups.tin.value"))\
        .withColumn("negotiation_arrangement",col("in_network.negotiation_arrangement"))\
        .drop("in_network","negotiated_rates","negotiated_prices","provider_groups","covered_services")


# COMMAND ----------

df_in_network_main.printSchema()

# COMMAND ----------

df_in_network_main.display()

# COMMAND ----------



# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# Create a SparkSession object
spark = SparkSession.builder.appName("CreateDataFrame").getOrCreate()

schema = StructType([
    StructField("Dataframe", StringType(), True),
    StructField("Row_Count", IntegerType(), True),
])

data = [("df_root",df_root.count()),
        ("df_in_network_main",df_in_network_main.count()),
       ]

df_row_count = spark.createDataFrame(data, schema)

# COMMAND ----------

df_row_count.display()

# COMMAND ----------


#Writing the in_network References data into parquet files
df_in_network_main.write.mode('append').parquet('dbfs:/mnt/poc/In_network')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


