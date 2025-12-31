import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def create_spark_connection():
    try:
        spark = (
            SparkSession.builder
            .appName("SparkKafkaDeltaStreaming")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session created successfully")
        return spark
    except Exception as e:
        logging.error(f"Could not create Spark session: {e}")
        return None

def connect_to_kafka(spark_conn):
    try:
        kafka_df = (
            spark_conn.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "broker:29092")
            .option("subscribe", "user_created")
            .option("startingOffsets", "earliest")
            .load()
        )
        return kafka_df
    except Exception as e:
        logging.error(f"Could not create Kafka DataFrame: {e}")
        return None

def create_selection_df_from_kafka(kafka_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("addusers", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False),
    ])

    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )
    return parsed_df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    spark = create_spark_connection()
    
    if spark:
        kafka_df = connect_to_kafka(spark)
        if kafka_df:
            users_df = create_selection_df_from_kafka(kafka_df)
            logging.info("Starting streaming to Delta Lake...")
            
            query = (
                users_df.writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", "/data/delta/checkpoints/user_created")
                .option("path", "/data/delta/user_created")
                .start()
            )
            query.awaitTermination()
