from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def merge_all_files() -> None:
    """lê todos os arquivos amazon e junta em um conjunto só parquet"""
    path = "file:/home/luciene/study/e-core"
    spark = SparkSession.builder.config("spark.driver.memory", "10g").appName("PySpark Test").getOrCreate()
    df = spark.read.parquet(f"{path}/entregavel_amazon/*/*")
    df = df.select(
        F.col("customer_id"),
        F.col("review_date"),
        F.col("review_id"),
        F.col("product_id"),
        F.col("product_parent"),
        F.col("product_title"),
        F.col("product_category"),
        F.col("star_rating"),
        F.col("helpful_votes"),
        F.col("total_votes"),
    )
    df = df.dropDuplicates()

    df.write.mode("overwrite").parquet(f"{path}/entregavel/amazon_data2")
    spark.stop()


def convert_to_parquet(source_path, dest_parh, rep_num) -> None:
    """lê o arquivos amazon tsv e salva em parquet. Recebe como parametros o nome do arquivo de origem, o nome da pasta destino e o número de particões"""
    path = "file:/home/luciene/study/e-core"
    spark = SparkSession.builder.appName("PySpark Test").getOrCreate()
    df = spark.read.format("csv").option("header", True).option("delimiter", "\t").load(f"{path}/{source_path}")
    df = df.repartition(rep_num)
    df.write.mode("overwrite").parquet(f"{path}/parquet/{dest_parh}")
    spark.stop()


if __name__ == '__main__':
    merge_all_files()
    convert_to_parquet("amazon_reviews_us_Digital_Video_Download_v1_00.tsv", "download_data", 15)
    convert_to_parquet("amazon_reviews_us_Video_DVD_v1_00.tsv", "dvd_data", 5)
    convert_to_parquet("amazon_reviews_us_Video_v1_00.tsv", "v1_data", 2)
    merge_all_files()
