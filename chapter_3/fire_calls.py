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

    # Read dataset and load into memory
    df = spark.read.csv(filePath, inferSchema=True, header=True)

    # Select with where condition
    (df
        .select("IncidentNumber", "AvailableDtTm", "CallType")
        .where(F.col("CallType") != "Medical Incident")
        .show(5, truncate=False))

    ## Aggregation (count)
    (df
        .select("CallType").where(F.col("CallType").isNotNull())
        .agg(F.countDistinct("CallType").alias("Distinct Call Types"))
        .show())

    ## Conversion (string to timestamp)
    print(df.withColumn("CallDate", F.to_timestamp(F.col("CallDate"), format="dd/MM/yyyy")).dtypes)

    ## Select year, month and day of timestamp
    (df
        .withColumn("CallDate", F.to_timestamp(F.col("CallDate"), format="dd/MM/yyyy"))
        .select(
            F.year("CallDate").alias("CallYear"),
            F.month("CallDate").alias("CallMonth"))
        .distinct()
        .orderBy(F.year("CallDate").asc())
        .show(10))

    ## What were all the different types of fire calls in 2018?
    (df
        .withColumn("CallDate", F.to_timestamp(F.col("CallDate"), format="dd/MM/yyyy"))
        .select("CallType")
        .where(F.year("CallDate") == "2018")
        .distinct()
        .show(10))
    
    # What months within the year 2018 saw the highest number of fire calls?
    (df
        .withColumn("CallDate", F.to_timestamp(F.col("CallDate"), format="dd/MM/yyyy"))
        .select("*")
        .where(F.year("CallDate") == "2018")
        .groupBy(F.month("CallDate").alias("Month"))
        .agg(F.count("*").alias("Number of calls"))
        .orderBy(F.col("Month").asc())
        .show())

    # Which neighborhood in San Francisco generated the most fire calls in 2018?
    (df
        .withColumn("CallDate", F.to_timestamp(F.col("CallDate"), format="dd/MM/yyyy"))
        .where((F.year("CallDate") == "2018") & (F.col("Zipcode").isNotNull()))
        .groupBy("Zipcode")
        .count()
        .orderBy("count", ascending=False)
        .show(10))

    # Which neighborhoods had the worst response times to fire calls in 2018?
    (df
        .withColumn("CallDate", F.to_timestamp(F.col("CallDate"), format="dd/MM/yyyy"))
        .groupBy("Neighborhood")
        .agg(F.avg("Delay").alias("Average Delay"))
        .orderBy("Average Delay", ascending=False)
        .show(1))
   

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: fire_calls.py %s", sys.stderr)
        sys.exit(-1)
    main(sys.argv[1])