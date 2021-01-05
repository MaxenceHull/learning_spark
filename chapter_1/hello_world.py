from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)
lines = sc.textFile("readme.md")
pythonLines = lines.filter(lambda line: "Examples" in line)
print(pythonLines.first())