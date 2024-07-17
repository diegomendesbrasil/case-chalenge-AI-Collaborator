# Databricks notebook source
# Importing libraries

import base64
import json
import requests
import time

from pyspark.sql.functions import regexp_replace, date_format
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import initcap, lit
from pandas import DataFrame
from re import findall
#from fastparquet import write 
import pandas as pd
from pyspark.sql.types import StringType

from datetime import datetime, timedelta

# Start time of the notebook processing
start_time = datetime.now()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Notebook for configurations and functions

# COMMAND ----------

# MAGIC %run
# MAGIC "../../Acess/Connections_Variable"

# COMMAND ----------

# Subject to be searched in the API

sourceFile = 'WorkItems'

# COMMAND ----------

# Creates a string containing today's date and uses it as a filter in the ChangedDate column

#today = datetime.now().strftime("%Y-%m-%d") EXAMPLE OF HOW TO GET 'TODAY'
reprocessamento = ''
yesterday = (datetime.now() - timedelta(1)).strftime("%Y-%m-%d") 

if reprocessamento != '':
  data_corte = reprocessamento
else:
  data_corte = yesterday

print(f'The date to be used in the filter is: {data_corte}')

# COMMAND ----------

# Fetches data from the API filtering ChangedDate by data_corte and returns a pandas dataframe. At the end of processing, it displays how many rows the dataframe has

dfOdata = getDadosDiarioAuroraAPI(sourceFile, data_corte)

# COMMAND ----------

# Captures the current date/time and inserts it as a new column in the dataframe

current_time = (datetime.now() - pd.DateOffset(hours=3)).strftime("%Y-%m-%d_%H_%M_%S")
dfOdata['DataCarregamento'] = current_time

# COMMAND ----------

# Creates the path where the file will be saved. Standard: datalake zone / notebook subject / yyyy-mm-dd_hh_mm_ss

sinkPath = aurora_raw_folder + sourceFile + '/' + current_time

# COMMAND ----------

# Transforms the pandas dataframe into a spark dataframe

df = spark.createDataFrame(dfOdata.astype(str))

# COMMAND ----------

# Saves the table in avro format at the specified path

df.write.mode('overwrite').format('avro').save(sinkPath)

# COMMAND ----------

end_time = datetime.now()
notebook_duration = str((end_time - start_time)).split('.')[0]
print(f'Notebook execution time: {notebook_duration}')

# COMMAND ----------

# End of daily raw WorkItems load
