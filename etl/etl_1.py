# Databricks notebook source
# MAGIC %md
# MAGIC Arquivo ETL que recebe como parâmetro o PATH do arquivo Netflix em CSV DESCOMPACTADO e a pasta onde irá salvar o arquivo parquet . Esse ETL deve ser executado para os 4. Poderia ser criado um método que chamasse todos, mas preferi deixar o widget para demostrar conhecimento dessa funcionalidade.

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
from pyspark.sql.window import Window

# COMMAND ----------

#utiliza variáveis para o path do arquivo, que pode ser de um volume que esteja no cloud storage (caso Databricks)
path_file = dbutils.widgets.get("path_file")
folder_name = dbutils.widgets.get("folder_name")

# COMMAND ----------

schema = StructType([
   StructField("customer_id", StringType(), True),
   StructField("rating_value", StringType(), True),
   StructField("rating_date", StringType(), True)
])

# COMMAND ----------

#é necessário passar o schema para que o csv seja lido corretamente. Caos não seja configurado, o df vai ter apenas a primeira coluna (primeiro registro tem o id do filme)
df_file = spark.read.format("csv").option("header", False).schema(schema).load(path_file)

# COMMAND ----------


#é incluída uma coluna com o numero da linha. Necessário para o determinação de qual filme pertence esse rating. O df vai ter apenas uma partição, o que é importante.
w = Window().orderBy(F.lit(1))
df = df_file.withColumn("row_num", F.row_number().over(w))



# COMMAND ----------

# o valor default por partição deve ser em torno de 128MB e pelo tamanho do arquivo, 2 partições são suficientes.
df = df.repartition(2)

# COMMAND ----------

#importante salvar o arquivo em parquet. Depois o mesmo pode ser carregado em uma table Delta, por exemplo
df.write.parquet(f"file:/tmp/output/{folder_name}")
