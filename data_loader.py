def init_spark():
    from pyspark.sql import SparkSession

    return SparkSession.builder \
        .appName("Profilage Données - Open Food Facts") \
        .config("spark.driver.memory", "4g") \
        .config("spark.hadoop.hadoop.security.authorization", "false") \
        .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
        .getOrCreate()


def load_data(path, spark):
    return spark.read.option("header", True) \
                     .option("inferSchema", True) \
                     .option("sep", "\t") \
                     .csv(path)
