from pyspark.sql import SparkSession
import time

start_time=time.time()
spark = SparkSession.builder.appName("q3b").getOrCreate()

movie_genres = spark.read.format('csv'). \
                     options(header = 'false',
                         inferSchema = 'true'). \
                     load("hdfs://master:9000/Excercise/movie_genres.csv")
movie_genres.registerTempTable("movie_genres")

ratings = spark.read.format('csv'). \
                options(header = 'false',
                     inferSchema = 'true'). \
                load("hdfs://master:9000/Excercise/ratings.csv")

ratings.registerTempTable("ratings")

sqlString = "select _c1 as MovieId, avg(_c2) as AverageRatePerMovie " + \
            "from ratings " + \
            "group by MovieId"

resu = spark.sql(sqlString)

resu.registerTempTable("rat")

sqlString2 = "select count(rat.MovieId), movie_genres._c1 as Category, " + \
               "avg(rat.AverageRatePerMovie) " + \
             "from rat " + \
             "inner join movie_genres " + \
             "on rat.MovieId = movie_genres._c0 " + \
             "group by Category order by Category"

res = spark.sql(sqlString2)

res.show()
print("--- %s seconds ---" % (time.time() - start_time))

