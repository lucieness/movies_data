from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
from delta import *


def save_netflix_movie_data_delta() -> None:
    """lê o arquivo movie_titles.csv da netflix e salva como uma tabela delta"""
    path = "file:/home/luciene/study/e-core"
    builder = SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    df_movie = spark.read.format("csv").option("header", False).load(f"{path}/movie_titles.csv")
    df_movie = df_movie.select(
        F.col("_c0").alias("product_id").cast(StringType()),
        F.col("_c1").alias("year_of_release"),
        F.col("_c2").alias("product_title")
    )
    df_movie.write.mode("overwrite").format("delta").save(f"{path}/db_files/delta/netflix_movie_data")
    spark.stop()

def save_all_movie_rating_delta() -> None:
    """lê os arquivos parquet tanto da netflix como da amazon e junta em um só"""
    path = "file:/home/luciene/study/e-core"
    builder = SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    df = spark.read.parquet(f"{path}/parquet/amazon_data/*")
    df1 = df.withColumn("platform", F.lit("AMZ")).select(
        F.col("customer_id"),
        F.col("product_id"),
        F.col("product_title"),
        F.col("review_date").alias("rating_date"),
        F.col("star_rating"),
        F.col("platform")
    )
    df1 = df1.dropDuplicates()

    df2 = spark.read.parquet(f"{path}/parquet/netflix_data/*")
    df2 = df2.withColumn("platform", F.lit("NFLX")).select(
        F.col("customer_id").alias("customer_id"),
        F.col("movie_id").alias("product_id"),
        F.col("title").alias("product_title"),
        F.col("rating_date"),
        F.col("rating_value").alias("star_rating"),
        F.col("platform")
    )
    df2 = df2.dropDuplicates()

    df3 = df1.unionByName(df2)
    df3.write.mode("overwrite").format("delta").save(f"{path}/db_files/delta/movie_star_rating")

    spark.stop()


if __name__ == '__main__':
    save_netflix_movie_data_delta()
    save_all_movie_rating_delta()
