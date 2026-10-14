import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (avg, col, count, from_json, lit,
                                   max as _max, min as _min,
                                   timestamp_seconds, window)
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
    # Convertir le timestamp Unix en vrai horodatage, requis pour les fenêtres
    planes = planes.withColumn("event_time", timestamp_seconds(col("timestamp")))

    holding = (planes
        .filter(~col("on_ground") & (col("altitude_m") < 6000))
        .withWatermark("event_time", "2 minutes")
        # OpenSky renvoie la même position tant qu'un avion n'a pas émis de mise à jour
        .dropDuplicates(["icao24", "event_time"])
        .groupBy(
            window(col("event_time"), "10 minutes", "5 minutes"),
            col("icao24"),
            col("callsign"),
        )
        .agg(
            count("*").alias("nb_positions"),
            (_max("latitude") - _min("latitude")).alias("delta_lat"),
            (_max("longitude") - _min("longitude")).alias("delta_lon"),
            avg("altitude_m").alias("altitude_moy"),
        )
        .filter(
            (col("nb_positions") >= 4)
            & (col("delta_lat") < 0.15)
            & (col("delta_lon") < 0.15)
        )
        .withColumn("alert", lit("HOLDING_PATTERN"))
    )

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

    q3 = (holding.writeStream
          .outputMode("append").format("console")
          .option("truncate", False).start())

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()