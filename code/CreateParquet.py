from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ParquetCreation").getOrCreate()
csv_data1 = spark.read.format("csv").option("header","false").option("inferSchema", "true").load("hdfs://master:9000/Excercise/movies.csv")
csv_data2 = spark.read.format("csv").option("header","false").option("inferSchema", "true").load("hdfs://master:9000/Excercise/movie_genres.csv")
csv_data3 = spark.read.format("csv").option("header","false").option("inferSchema", "true").load("hdfs://master:9000/Excercise/ratings.csv")

csv_data1.write.mode('overwrite').parquet("hdfs://master:9000/Excercise/movies.parquet")
csv_data2.write.mode('overwrite').parquet("hdfs://master:9000/Excercise/movie_genres.parquet")
csv_data3.write.mode('overwrite').parquet("hdfs://master:9000/Excercise/ratings.parquet")
