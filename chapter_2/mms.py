import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions

def main(file_path):
    spark = (SparkSession
        .builder
        .appName("MnMsCount")
        .getOrCreate())
    
    # set log level to warning and errors only
    spark.sparkContext.setLogLevel('WARN')

    # read the file into a Spark DataFrame
    mnm_df = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(file_path))
    mnm_df.show(n=5, truncate=False)

    # aggregate count of all colors and groupBy state and color
    # orderBy descending order
    count_mnm_df = (mnm_df.select("State", "Color", "Count")
                    .groupBy("State", "Color")
                    .sum("Count")
                    .orderBy("sum(Count)", ascending=False))

    # show all the resulting aggregation for all the dates and colors
    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

    # find the aggregate count for California by filtering
    ca_count_mnm_df = (mnm_df.select("*")
                       .where(mnm_df.State == 'CA')
                       .groupBy("State", "Color")
                       .sum("Count")
                       .orderBy("sum(Count)", ascending=False)
                       .withColumnRenamed("sum(Count)", "Total"))

    # show the resulting aggregation for California
    ca_count_mnm_df.show(n=10, truncate=False)
    
    # Create expression
    ca_count_mnm_df.select(functions.expr("Total * 2")).show(truncate=False)
    ca_count_mnm_df.withColumn("Small sum", functions.expr("Total < 90000")).show(truncate=False)
    ca_count_mnm_df.sort(functions.col("Color").desc()).show(truncate=False)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mms.py %s", sys.stderr)
        sys.exit(-1)
    main(sys.argv[1])