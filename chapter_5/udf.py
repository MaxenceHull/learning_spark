from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def cube(s):
    return s * s * s

def main():
    # Initialize spark
    spark = (SparkSession
        .builder
        .appName("SFFireCalls")
        .getOrCreate())
    
    # set log level to warning and errors only
    spark.sparkContext.setLogLevel('WARN')

    # register user defined function
    spark.udf.register("cube", cube)

    # use UDF
    spark.range(1, 9).createOrReplaceTempView("udf_test")
    spark.sql("SELECT id, cube(id) AS id_cubed FROM udf_test").show()

if __name__ == "__main__":
    main()