# Databricks notebook source
# MAGIC %md
# MAGIC Arquivo ETL que recebe como parâmetro o PATH da pasta onde estáo os dados Netflix, já em parquet. Temos 4 arquivos combined_data. Esse ETL deve ser executado para os 4. Poderia ser criado um método que chamasse todos, mas preferi deixar o widget para demostrar conhecimento dessa funcionalidade.

# COMMAND ----------

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructType, StructField, IntegerType

# COMMAND ----------

folder_path = dbutils.widgets.get("folder_path")

# COMMAND ----------

schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("rating_value", StringType(), True),
        StructField("rating_date", StringType(), True),
        StructField("row_num", IntegerType(), True),
        StructField("movie_id", StringType(), True)
    ])

# COMMAND ----------

# combina todos os arquivos parquets em um único DF
df = spark.read.parquet(folder_path)
df = df.dropDuplicates()

# COMMAND ----------

df = df.withColumn("row_num", F.col("row_num").cast(IntegerType()))


# COMMAND ----------

#recupera todos os registros que são na verdade ids do filme
movie_id_rows = df.where("rating_value IS NULL").orderBy("row_num").collect()

# COMMAND ----------

total_count_movies = len(movie_id_rows)
index_movie_id = 0
buffer = 1
MAX_BUFFER_SIZE = 1000
df_buffer = spark.createDataFrame([], schema=schema)

# COMMAND ----------

while index_movie_id < total_count_movies - 1:
    row1 = movie_id_rows[index_movie_id]
    row2 = movie_id_rows[index_movie_id + 1]
    df_single_movie = df.where((df.row_num > row1.row_num) & (df.row_num < row2.row_num))
    df_single_movie = df_single_movie.withColumn("movie_id", F.lit(row1.customer_id.strip(":")))

    print(f"{row1.customer_id}: from {row1.row_num} to {row2.row_num}")

    df_buffer = df_buffer.union(df_buffer)
    if buffer == MAX_BUFFER_SIZE:
        buffer = 0
        # o union dentro do looping vai resultar em várias partições. Pelo tamanho do arquivo é possível
        # fazer um coalesce para 1 partição só.
        df_buffer = df_right.coalesce(1)

        #salva em um staging area
        df_buffer.write.parquet(f"file:/tmp/staging_area_netflix/{row2.customer_id.strip(':')}")
        df_buffer = spark.createDataFrame([], schema=schema)
        print("buffer liberado")
    else:
        buffer = buffer + 1
    index_movie_id = index_movie_id + 1

# COMMAND ----------

#vao faltar os dados do último filme, então é necessario fazer esse ultimo passo
df_last_movie = df.where(df.row_num > movie_id_rows[-1].row_num)
df_last_movie = df_last_movie.withColumn("movie_id", F.lit(movie_id_rows[-1].customer_id.strip(":")))

df_buffer = df_buffer.union(df_aux_last)
df_buffer = df_buffer.coalesce(1)
df_buffer.write.parquet(f"file:/tmp/staging_area_netflix/{movie_id_rows[-1].customer_id.strip(':')}")
