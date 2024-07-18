import os
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

spark = SparkSession.builder \
    .appName("Consume ETL WorkItem") \
    .master("spark://localhost:7077") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .getOrCreate()

sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://localhost:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

source_path = 's3a://project-ai-colaborator/standardized/WorkItems/'
df = spark.read.parquet(source_path)

df.createOrReplaceTempView("workitems")

dimworkitem = spark.sql("""
    SELECT
        WorkItemId AS work_item_id,
        Title AS title,
        WorkItemType AS work_item_type,
        State AS state,
        Priority AS priority,
        AssignedToUserSK AS assigned_to_user_sk,
        CreatedDate AS created_date,
        ClosedDate AS closed_date,
        ResolvedDate AS resolved_date,
        DataCarregamento AS data_load
    FROM workitems
""")

sink_path = 's3a://project-ai-colaborator/consume/DimWorkItem/'
dimworkitem.write.mode('overwrite').format('parquet').save(sink_path)
print(f'Dimensão WorkItem salva em {sink_path} com sucesso!')