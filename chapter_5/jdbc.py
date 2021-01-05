from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    # Initialize spark
    spark = (SparkSession
        .builder
        .appName("SFFireCalls")
        .getOrCreate())
    
    # set log level to warning and errors only
    spark.sparkContext.setLogLevel('WARN')

    # Load and display regions and depatments
    regions = (spark.
        read.
        format("jdbc").
        option("url", "jdbc:postgresql://localhost/").
        option("driver", "org.postgresql.Driver").
        option("dbtable", "regions").
        option("user", "postgres").
        option("password", 'pwd').
        option("encoding", "UTF-8").
        load())
    
    regions = regions.withColumnRenamed("id", "regions_id")
    regions = regions.withColumnRenamed("code", "regions_code")
    regions = regions.withColumnRenamed("capital", "regions_capital")
    regions = regions.withColumnRenamed("name", "regions_name")
    
    departements = (spark.
        read.
        format("jdbc").
        option("url", "jdbc:postgresql://localhost/").
        option("driver", "org.postgresql.Driver").
        option("dbtable", "departments").
        option("user", "postgres").
        option("password", 'pwd').
        option("encoding", "UTF-8").
        load())
    
    departements = departements.withColumnRenamed("id", "dpt_id")
    departements = departements.withColumnRenamed("code", "dpt_code")
    departements = departements.withColumnRenamed("capital", "dpt_capital")
    departements = departements.withColumnRenamed("name", "dpt_name")
    departements = departements.withColumnRenamed("region", "dpt_region")
    
    all = regions.join(departements, on=departements.dpt_region == regions.regions_code)

    # Which is the region of the Landes departments (40)?
    all.select(F.col("regions_name")).where(F.col("dpt_code") == 40).show()

    # How many cities in per department in the Aquitaine region?
    aquitaine = all.where(F.col("regions_code") == 72)
    cities = (spark.
        read.
        format("jdbc").
        option("url", "jdbc:postgresql://localhost/").
        option("driver", "org.postgresql.Driver").
        option("dbtable", "towns").
        option("user", "postgres").
        option("password", 'pwd').
        option("encoding", "UTF-8").
        load())

    aquitaine_cities = aquitaine.join(cities, on=aquitaine.dpt_code == cities.department)
    aquitaine_cities.groupBy("dpt_name").agg(F.countDistinct("code").alias("Number_of_cities")).show()

    # Do it the SQL-way
    
    # Create tables
    aquitaine.write.mode("overwrite").saveAsTable("dpts")
    cities.write.mode("overwrite").saveAsTable("cities")

    # Run query
    spark.sql("""
        SELECT dpt_name, count(code) as Number_of_cities
        FROM dpts
        INNER JOIN cities ON dpts.dpt_code = cities.department
        GROUP BY dpt_name
    """).show()


if __name__ == "__main__":
    main()