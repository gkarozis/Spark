from pyspark.sql import SparkSession
import time

start_time= time.time()

spark = SparkSession.builder.appName("q2b").getOrCreate()

ratings = spark.read.format('csv'). \
                options(header = 'false',
                     inferSchema = 'true'). \
                load("hdfs://master:9000/Excercise/ratings.csv")
ratings.registerTempTable("ratings")

sqlString = "select count(Distinct User) as NoUsers " + \
            "from (select _c0 as User, avg(_c2) as rate " + \
            "from ratings " + \
            "group by User order by User) "+ \
            "where rate>=3" 

sqlString2 = "select count(Distinct _c0) as NoUsers " + \
            "from ratings"

res = spark.sql(sqlString)
res2 = spark.sql(sqlString2)
res.show()    
res2.show()

print(" --- %s seconds ---" % (time.time()-start_time))

