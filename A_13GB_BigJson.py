# Databricks notebook source
# MAGIC %md
# MAGIC ##### Reading 1st file which is 13.16 GB, Filename - 2023-01-06_BCBSMI_IH0000000025_ffs_in-network.json.gz

# COMMAND ----------

# df1 reads the file size of 13.16GB json file

df1 = spark.read.option('multiline',True).json("dbfs:/mnt/BigJson2023/Big_Json_Files_Poc/2023-01-06_BCBSMI_IH0000000025_ffs_in-network.json.gz")

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

from pyspark.sql.functions import explode, col

main_dm = df1.withColumn("in_network", explode("in_network"))\
        .withColumn("billing_code", col("in_network.billing_code"))\
        .withColumn("billing_code_type", col("in_network.billing_code_type"))\
        .withColumn("billing_code_type_version", col("in_network.billing_code_type_version"))\
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
        .withColumn("service_code",explode("negotiated_prices.service_code"))\
        .withColumn("provider_groups",explode("negotiated_rates.provider_groups"))\
        .withColumn("npi",explode("provider_groups.npi"))\
        .withColumn("tin", col("provider_groups.tin.type"))\
        .withColumn("tin_val",col("provider_groups.tin.value"))\
        .withColumn("negotiation_arrangement",col("in_network.negotiation_arrangement"))\
        .withColumn("last_updated_on", col("last_updated_on"))\
        .withColumn("reporting_entity_name", col("reporting_entity_name"))\
        .withColumn("reporting_entity_type", col("reporting_entity_type"))\
        .withColumn("version", col("version"))\
        .drop("in_network","negotiated_rates","negotiated_prices","provider_groups")



# COMMAND ----------

main_dm.printSchema()

# COMMAND ----------

main_dm.display()

# COMMAND ----------

# main_dm.write.saveAsTable("13GB_MA_flattned")
main_dm.write.csv("dbfs:/FileStore/tables/13GB_A_flattned.csv", header=True, mode="overwrite")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ##### Reading 2nd file which is 15.05 GB, Filename - 2023-02_748_29M0_in-network-rates_1_of_2.json.gz

# COMMAND ----------

df_15_05GB =  spark.read.option('multiline',True).json("dbfs:/mnt/BigJson2023/Big_Json_Files_Poc/2023-02_748_29M0_in-network-rates_1_of_2.json.gz")     

# COMMAND ----------

df_15_05GB.printSchema()

# COMMAND ----------



# COMMAND ----------

main_15_05GB=df_15_05GB.withColumn("in_network", explode("in_network"))\
        .withColumn("billing_code", col("in_network.billing_code"))\
        .withColumn("billing_code_type",col("in_network.billing_code_type"))\
        .withColumn("billing_code_type_version",col("in_network.billing_code_type_version"))\
        .withColumn("bundled_codes",explode("in_network.bundled_codes"))\
        .withColumn("billing_code",col("bundled_codes.billing_code"))\
        .withColumn("billing_code_type",col("bundled_codes.billing_code_type"))\
        .withColumn("billing_code_type_version",col("bundled_codes.billing_code_type_version"))\
        .withColumn("description",col("bundled_codes.description"))\
        .withColumn("name",col("in_network.name"))\
        .withColumn("negotiated_rates",explode("in_network.negotiated_rates"))\
        .withColumn("negotiated_prices",explode("negotiated_rates.negotiated_prices"))\
        .withColumn("billing_class",col("negotiated_prices.billing_class"))\
        .withColumn("billing_code_modifier",explode("negotiated_prices.billing_code_modifier"))\
        .withColumn("expiration_date",col("negotiated_prices.expiration_date"))\
        .withColumn("negotiated_rate",col("negotiated_prices.negotiated_rate"))\
        .withColumn("negotiated_type",col("negotiated_prices.negotiated_type"))\
        .withColumn("service_code",explode("negotiated_prices.service_code"))\
        .withColumn("negotiation_arrangement",col("in_network.negotiation_arrangement"))\
        .withColumn("last_updated_on",col("last_updated_on"))\
        .withColumn("Prrovider_references",explode("provider_references"))\
        .withColumn("provider_group_id",col("Prrovider_references.provider_group_id"))\
        .withColumn("provider_groups",explode("Prrovider_references.provider_groups"))\
        .withColumn("npi",explode("provider_groups.npi"))\
        .withColumn("tin",col("provider_groups.tin.type"))\
        .withColumn("value",col("provider_groups.tin.value"))\
        .withColumn("reporting_entity_name",col("reporting_entity_name"))\
        .withColumn("reporting_entity_type",col("reporting_entity_type"))\
        .withColumn("version",col("version"))\
.drop("in_network","provider_references","bundled_codes","negotiated_rates","negotiated_prices","Prrovider_references","provider_groups")


# COMMAND ----------

main_15_05GB.printSchema()

# COMMAND ----------

main_15_05GB

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


