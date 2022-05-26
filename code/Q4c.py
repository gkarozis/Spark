from pyspark.sql import SparkSession
import time

start_time=time.time()
spark = SparkSession.builder.appName("q4c").getOrCreate()

movies = spark.read.format('parquet'). \
               options(header = 'false',
                    inferSchema = 'true'). \
               load("hdfs://master:9000/Excercise/movies.parquet")

movies.registerTempTable("movies")

movie_genres = spark.read.format('parquet'). \
                     options(header = 'false',
                    inferSchema = 'true'). \
                    load("hdfs://master:9000/Excercise/movie_genres.parquet")

movie_genres.registerTempTable("movie_genres")

def get_dec(ts):
    if ts>=2000 and ts<=2004:
         return 2000
    elif ts >=2005 and ts <=2009:
         return 2005
    elif ts >=2010 and ts <= 2014:
         return 2010
    else:
         return 2015

def get_year(ts):
    return ts.year

def word_count(summ):
    return len(list(summ.split(" ")))

def get_length(summ):
    return type(summ)
# len(list(sum.split(" ")))

spark.udf.register("year", get_year)
spark.udf.register("dec", get_dec)
spark.udf.register("leng", get_length)

sqlString = "select _c0 as MovieId, "+ \
            "(Length(_c2)-Length(replace(_c2, ' ',''))+1) as SummaryLen, "+ \
            "dec(year(_c3)) as Decade "+ \
            "from movies "+ \
            "where _c3 is not null and year(_c3)>=2000"

sqls = "select _c0 as MovieId, _c1 as Category " +\
       "from movie_genres where _c1 = 'Drama'"

res3 = spark.sql(sqls)

res3.registerTempTable("sqls")

res = spark.sql(sqlString)

res.registerTempTable("meso")

sqlString2 = "select "+ \
             "avg(meso.SummaryLen), meso.Decade " + \
             "from meso " + \
             "inner join sqls " + \
             "on meso.MovieId = sqls.MovieId "+ \
             "group by Decade order by Decade"

res2 = spark.sql(sqlString2)

res2.show()
print("--- %s seconds ---" % (time.time() - start_time))
