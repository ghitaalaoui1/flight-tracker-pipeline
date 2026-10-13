import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import (BooleanType, DoubleType, LongType, StringType,
                               StructField, StructType)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")
TOPIC = "flight-positions"

SCHEMA = StructType([
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("country", StringType()),
    StructField("timestamp", LongType()),
    StructField("longitude", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("altitude_m", DoubleType()),
    StructField("on_ground", BooleanType()),
    StructField("speed_kmh", DoubleType()),
    StructField("vertical_rate_ms", DoubleType()),
    StructField("squawk", StringType()),
])


def main():
    spark = SparkSession.builder.appName("FlightAlerts").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "latest")
           .load())

    planes = raw.select(
        from_json(col("value").cast("string"), SCHEMA).alias("d")
    ).select("d.*")

    emergencies = (planes
                   .filter(col("squawk").isin("7500", "7600", "7700"))
                   .withColumn("alert", lit("EMERGENCY")))

    low_and_slow = planes.filter(
        (col("altitude_m") < 1500)
        & (col("speed_kmh") < 400)
        & (~col("on_ground"))
    ).withColumn("alert", lit("LOW_AND_SLOW"))

    q1 = (emergencies.writeStream
          .outputMode("append").format("console")
          .option("truncate", False).start())

    q2 = (low_and_slow.writeStream
          .outputMode("append").format("console")
          .option("truncate", False).start())

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()