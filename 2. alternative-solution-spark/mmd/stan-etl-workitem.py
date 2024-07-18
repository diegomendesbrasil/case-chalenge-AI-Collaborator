import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, datediff
from pyspark.sql.types import IntegerType, DoubleType

spark = SparkSession.builder \
    .appName("Standardize ETL WorkItem") \
    .master("spark://localhost:7077") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .getOrCreate()

sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://localhost:9001")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

source_path = 's3a://project-ai-colaborator/raw/WorkItems/'
df = spark.read.parquet(source_path)

df_standardized = df.select(
    col('WorkItemId'),
    to_date(col('InProgressDate')).alias('InProgressDate'),
    to_date(col('CompletedDate')).alias('CompletedDate'),
    col('LeadTimeDays').cast(DoubleType()),
    col('CycleTimeDays').cast(DoubleType()),
    col('InProgressDateSK').cast(IntegerType()),
    col('CompletedDateSK').cast(IntegerType()),
    col('AnalyticsUpdatedDate'),
    col('ProjectSK'),
    col('WorkItemRevisionSK'),
    col('AreaSK'),
    col('IterationSK'),
    col('AssignedToUserSK'),
    col('ChangedByUserSK'),
    col('CreatedByUserSK'),
    col('ActivatedByUserSK'),
    col('ClosedByUserSK'),
    col('ResolvedByUserSK'),
    col('ActivatedDateSK').cast(IntegerType()),
    col('ChangedDateSK').cast(IntegerType()),
    col('ClosedDateSK').cast(IntegerType()),
    col('CreatedDateSK').cast(IntegerType()),
    col('ResolvedDateSK').cast(IntegerType()),
    col('StateChangeDateSK').cast(IntegerType()),
    col('Revision').cast(IntegerType()),
    col('Watermark'),
    col('Title'),
    col('WorkItemType'),
    to_date(col('ChangedDate')).alias('ChangedDate'),
    to_date(col('CreatedDate')).alias('CreatedDate'),
    col('State'),
    col('Reason'),
    col('IntegrationBuild'),
    to_date(col('ActivatedDate')).alias('ActivatedDate'),
    col('Activity'),
    to_date(col('ClosedDate')).alias('ClosedDate'),
    col('Issue'),
    col('Priority').cast(IntegerType()),
    col('Rating'),
    to_date(col('ResolvedDate')).alias('ResolvedDate'),
    col('StackRank').cast(DoubleType()),
    col('CompletedWork').cast(DoubleType()),
    col('Effort').cast(DoubleType()),
    to_date(col('FinishDate')).alias('FinishDate'),
    col('RemainingWork').cast(DoubleType()),
    to_date(col('StartDate')).alias('StartDate'),
    to_date(col('TargetDate')).alias('TargetDate'),
    col('ParentWorkItemId').cast(IntegerType()),
    col('TagNames'),
    col('StateCategory'),
    col('AutomatedTestId'),
    col('AutomatedTestName'),
    col('AutomatedTestStorage'),
    col('AutomatedTestType'),
    col('AutomationStatus'),
    to_date(col('StateChangeDate')).alias('StateChangeDate'),
    col('Count').cast(IntegerType()),
    col('CommentCount').cast(IntegerType()),
    col('Microsoft_VSTS_CodeReview_AcceptedBySK'),
    to_date(col('Microsoft_VSTS_CodeReview_AcceptedDate')).alias('Microsoft_VSTS_CodeReview_AcceptedDate'),
    col('Microsoft_VSTS_CodeReview_ClosedStatus'),
    col('Microsoft_VSTS_CodeReview_ClosedStatusCode'),
    col('Microsoft_VSTS_CodeReview_ClosingComment'),
    col('Microsoft_VSTS_CodeReview_Context'),
    col('Microsoft_VSTS_CodeReview_ContextCode'),
    col('Microsoft_VSTS_CodeReview_ContextOwner'),
    col('Microsoft_VSTS_CodeReview_ContextType'),
    col('Microsoft_VSTS_Common_ReviewedBySK'),
    col('Microsoft_VSTS_Common_StateCode'),
    col('Microsoft_VSTS_Feedback_ApplicationType'),
    col('Microsoft_VSTS_TCM_TestSuiteType'),
    col('Microsoft_VSTS_TCM_TestSuiteTypeId'),
    col('DataCarregamento')
)

sink_path = 's3a://project-ai-colaborator/standardized/WorkItems/'
df_standardized.write.mode('overwrite').format('parquet').save(sink_path)
print(f'DataFrame padronizado salvo em {sink_path} com sucesso!')