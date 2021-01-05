import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main(filePath):
    # Initialize spark
    spark = (SparkSession
        .builder
        .appName("SFFireCalls")
        .getOrCreate())
    
    # set log level to warning and errors only
    spark.sparkContext.setLogLevel('WARN')

    # Read dataset and load it into memory
    df = spark.read.csv(filePath, inferSchema=True, header=True)
    df.createOrReplaceTempView("us_delay_flights_tbl")

    # Find all flights with distance greather than 1000 miles
    spark.sql("""SELECT Distance, Origin, Dest FROM us_delay_flights_tbl WHERE Distance > 1000 ORDER BY Distance DESC""").show(10)
    df.select("Distance", "Origin", "Dest").where(F.col("Distance") > 1000).orderBy("Distance", ascending=False).show(10)

    # Find all flights between San Francisco (SFO) and Chicago (ORD) with at least a two-hour delay
    spark.sql("""
        SELECT Origin, Dest, ArrDelay + DepDelay
        FROM us_delay_flights_tbl
        WHERE (Origin = 'SFO' AND Dest = 'ORD')
        AND ArrDelay + DepDelay > 120
    """).show(10)

    (df
        .select("Origin", "Dest", F.expr("ArrDelay + DepDelay"))
        .where( (F.col("Origin") == 'SFO') & (F.col("Dest") == 'ORD') & (F.col("ArrDelay") + F.col("DepDelay") > 120) )
        .show(10))

    # Find months when important delays are more common
    spark.sql("""
        SELECT Month, count(*) AS delayed_flights
        FROM us_delay_flights_tbl
        WHERE ArrDelay + DepDelay > 120
        GROUP BY Month
        ORDER BY delayed_flights DESC
    """).show()

    (df
        .where(F.col("ArrDelay") + F.col("DepDelay") > 120)
        .groupBy("Month")
        .agg(F.count("*").alias("delayed_flights"))
        .orderBy("delayed_flights", ascending=False)
        .show())

    # Categorize flight delays
    spark.sql("""
        SELECT ArrDelay, Origin, Dest, 
        CASE
            WHEN ArrDelay > 360 THEN 'Very Long Delays'
            WHEN ArrDelay > 120 AND ArrDelay < 360 THEN 'Long Delays'
            WHEN ArrDelay > 60 AND ArrDelay < 120 THEN 'Short Delays'
            WHEN ArrDelay > 0 and ArrDelay < 60  THEN  'Tolerable Delays'
            WHEN ArrDelay = 0 THEN 'No Delays'
            ELSE 'Early'
        END AS Flight_Delays
        FROM us_delay_flights_tbl
        ORDER BY Origin, ArrDelay DESC""").show(10)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: fire_calls.py %s", sys.stderr)
        sys.exit(-1)
    main(sys.argv[1])