# Databricks notebook source
# Importing libraries

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Configuration and functions notebook

# COMMAND ----------

# MAGIC %run
# MAGIC "../../Acess/Connections_Variable"

# COMMAND ----------

# Subject to be searched in the Datalake and creation of the path to read the files

sourceFile = 'WorkItems'
sourcePath = aurora_standardized_folder + sourceFile + '/'

# COMMAND ----------

# Loop through all folders of the subject in the Datalake to identify the one that contains the most recent records

max_data = ''
for i in dbutils.fs.ls(sourcePath):
  if i.name > max_data:
    max_data = i.name
    
sourcePath = sourcePath + max_data
print(sourcePath)

# COMMAND ----------

# Create the path where the file will be saved. Standard: datalake zone / notebook subject

sinkPath = aurora_consume_folder + sourceFile
print(sinkPath)

# COMMAND ----------

# Read the Avro file from the path and save it in a Spark dataframe

df = spark.read.parquet(sourcePath)

# COMMAND ----------

# Save the table in parquet format at the specified path

df.write.mode('overwrite').format('parquet').save(sinkPath)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Database writing for contingency activity
# MAGIC 1. Fetch data saved in the consume zone
# MAGIC 2. Create a DataFrame with this data
# MAGIC 3. Write the data to the first table of the WorkItems loading process

# COMMAND ----------

host_name = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001HostName")
port = 1433
database = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001DatabaseName")
user = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001UserName")
password = dbutils.secrets.get(scope = "key-vault-secrets", key = "SqlGto001DBPass")

# COMMAND ----------

DimWorkItem = spark.read.parquet(sinkPath)

# COMMAND ----------

urlgrava = f'jdbc:sqlserver://{host_name}:{port};databaseName={database}'

DimWorkItem.write\
    .format("jdbc")\
    .mode("overwrite")\
    .option("url", urlgrava)\
    .option("dbtable", "dbo.DimWorkItemTemp")\
    .option("user", user)\
    .option("password", password)\
    .save()

# COMMAND ----------

# End of Consume WorkItems load
