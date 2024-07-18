import os
import requests
import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime
import base64
import json

spark = SparkSession.builder \
    .appName("Raw ETL WorkItem") \
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

def get_dados_devops_api(source):
    df_odata = pd.DataFrame([])
    base_url = 'https://analytics.dev.azure.com/betsdcs/BetsDCI/_odata/'
    continuation_token = 0
    last_continuation_token = 0
    user_pass = "UserDiego:d2kvrc2uxrhdxnwncvjl5tdypy2smyxukhozyctc4t4sne2j4ajq"
    b64_user_pass = base64.b64encode(user_pass.encode()).decode()

    headers = {
        'Authorization': 'Basic %s' % b64_user_pass,
        'Accept': 'application/json'
    }

    while True:
        try:
            request_url = base_url + source + '?&$skiptoken=' + str(continuation_token)
            response = requests.get(request_url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                df = pd.json_normalize(data['value'])
                df_odata = pd.concat([df_odata, df])
                if data.get("@odata.nextLink"):
                    if last_continuation_token == int(data.get("@odata.nextLink").split('skiptoken=')[1]):
                        break
                    else:
                        continuation_token = int(data.get("@odata.nextLink").split('skiptoken=')[1])
                        last_continuation_token = continuation_token
                else:
                    break
            else:
                print(f'Erro: {response.content}')
                break
        except Exception as e:
            print(f'Exceção: {e}')
            break
    return df_odata

if __name__ == "__main__":
    source_file = 'WorkItems'
    df_odata = get_dados_devops_api(source_file)
    hora_atual = datetime.now().strftime("%Y-%m-%d_%H_%M_%S")
    df_odata['DataCarregamento'] = hora_atual
    sink_path = f's3a://project-ai-colaborator/raw/{source_file}/'

    df = spark.createDataFrame(df_odata.astype(str))
    df.show()
    try:
        df.write.mode('overwrite').format('parquet').save(sink_path)
        print(f'DataFrame salvo em {sink_path} com sucesso!')
    except Exception as e:
        print(f"Erro ao salvar DataFrame no MinIO: {e}")