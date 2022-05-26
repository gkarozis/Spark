from pyspark.sql import SparkSession
import time

start_time = time.time()
spark = SparkSession.builder.appName("q5b").getOrCreate()

rate = spark.read.format('csv'). \
                options(header = 'false',
                    inferSchema = 'true'). \
                load("hdfs://master:9000/Excercise/ratings.csv")

movies = spark.read.format('csv'). \
               options(header = 'false',
                    inferSchema = 'true'). \
               load("hdfs://master:9000/Excercise/movies.csv")

movie_genres = spark.read.format('csv'). \
                      options(header = 'false',
                    inferSchema = 'true'). \
                 load("hdfs://master:9000/Excercise/movie_genres.csv")

rate.registerTempTable("rate")

movies.registerTempTable("movies")

movie_genres.registerTempTable("movie_genres")

sqlString = "select movies._c0 as MovieId, " + \
            "movie_genres._c1 as Category, "+ \
            "movies._c1 as MovieName, "+ \
            "movies._c7 as MovieRates "+ \
            "from movies, movie_genres "+\
            "where movie_genres._c0=movies._c0"

mc = spark.sql(sqlString)
mc.registerTempTable("mc")

sqlString2 = "select movie_genres._c1 as Category, "+ \
             "rate._c0 as User, "+ \
             "count(rate._c1) as rates, max(rate._c2) as max, "+\
             "min(rate._c2) as min "+\
             "from rate, movie_genres " + \
             "where rate._c1=movie_genres._c0 " + \
             "group by Category, User"

rc = spark.sql(sqlString2)

rc.registerTempTable("rc")

sqlString3 = "select new.Category, new.MaxRates, rc.User, rc.max, rc.min "+ \
             "from (select Category, max(rates) as MaxRates "+ \
            "from rc "+ \
            "group by Category) as new, rc "+ \
            "where rc.rates = new.MaxRates and rc.Category = new.Category" 

max_rates = spark.sql(sqlString3)
max_rates.registerTempTable("mr")

sqlString4 = "select mr.Category, mr.MaxRates, mr.User, rate._c2 as Rate, "+ \
             "mc.MovieId, mc.MovieRates, mc.MovieName, mr.max " + \
             "from mc, mr, rate " + \
             "where mr.User=rate._c0 and mc.MovieId=rate._c1 and "+\
             "mr.max=rate._c2 and mr.Category = mc.Category "

res = spark.sql(sqlString4) 
res.registerTempTable("res")
res.show()

sqlString5 = "select Category as Cat, "+\
             "max(MaxRates) as MaxUserRates "+\
             "from res " +\
             "group by Category"

res2 = spark.sql(sqlString5)
res2.registerTempTable("res2")


sqlString6 = "select res.Category, res2.MaxUserRates, res.User, res.Rate, "+\
             "res.MovieId, res.MovieRates, res.MovieName "+\
             "from res2,res "+\
             "where res2.Cat = res.Category and "+\
             "res2.MaxUserRates=res.MaxRates "
 
res3 = spark.sql(sqlString6)
res3.registerTempTable("res3")

sqlString7 = "select Category as Cat,max(MovieRates) as MaxMovieRates "+ \
              "from res3 "+ \
              "group by Category "

res4 = spark.sql(sqlString7)
res4.registerTempTable("res4")

sqlString8 = "select res3.Category, res3.User, res3.MaxUserRates, "+ \
             "res3.Rate, res3.MovieName, res4.MaxMovieRates "+ \
             "from res3, res4 "+\
             "where res3.Category=res4.Cat and res3.MovieRates=res4.MaxMovieRates"

res5 = spark.sql(sqlString8)
res5.registerTempTable("res5")
res5.show()

sqlString9 = "select mr.Category, mr.MaxRates, mr.User, rate._c2 as Rate, "+ \
             "mc.MovieId, mc.MovieRates, mc.MovieName, mr.min " + \
             "from mc, mr, rate " + \
             "where mr.User=rate._c0 and mc.MovieId=rate._c1 and "+\
             "mr.min=rate._c2 and mr.Category = mc.Category "

res6 = spark.sql(sqlString9)
res6.registerTempTable("res6")

sqlString10 = "select Category as Cat, "+\
             "max(MaxRates) as MaxUserRates "+\
             "from res6 " +\
             "group by Category"

res7 = spark.sql(sqlString10)
res7.registerTempTable("res7")


sqlString11 = "select res6.Category, res7.MaxUserRates, res6.User, res6.Rate, "+\
             "res6.MovieId, res6.MovieRates, res6.MovieName "+\
             "from res7,res6 "+\
             "where res7.Cat = res6.Category and "+\
             "res7.MaxUserRates=res6.MaxRates "

res8 = spark.sql(sqlString11)
res8.registerTempTable("res8")

sqlString12 = "select Category as Cat,max(MovieRates) as MaxMovieRates "+ \
              "from res8 "+ \
              "group by Category "

res9 = spark.sql(sqlString12)
res9.registerTempTable("res9")

sqlString13 = "select res8.Category as Cat, res8.User as Us, res8.MaxUserRates as MUR, "+ \
             "res8.Rate as MinRate, res8.MovieName as MinMovieName, res9.MaxMovieRates "+ \
             "from res8, res9 "+\
             "where res8.Category=res9.Cat and res8.MovieRates=res9.MaxMovieRates"

res10 = spark.sql(sqlString13)
res10.registerTempTable("res10")

sqlStringF = "select res5.Category, res5.User, res5.MaxUserRates, "+\
             "res5.MovieName as MaxMovieName, res5.Rate as MaxRate,"+\
             "res10.MinMovieName, res10.MinRate "+\
             "from res5, res10 "+\
             "where res5.Category = res10.Cat "+\
             "order by category" 

res11 = spark.sql(sqlStringF)
res11.show()
print("--- %s seconds ---" % (time.time() - start_time))
