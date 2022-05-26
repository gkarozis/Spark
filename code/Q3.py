from pyspark.sql import SparkSession
import time

start_time = time.time()
spark = SparkSession.builder.appName("quer3-rdd").getOrCreate()

sc = spark.sparkContext


kind = sc.textFile("hdfs://master:9000/Excercise/movie_genres.csv"). \
          map(lambda x : (int(x.split(",")[0]),x.split(",")[1])). \
          sortByKey()

rate = sc.textFile("hdfs://master:9000/Excercise/ratings.csv",100). \
          map(lambda x: (int(x.split(",")[1]),(float(x.split(",")[2]),1))). \
          reduceByKey(lambda x, y: (x[0]+y[0],x[1]+y[1])). \
          map(lambda x : (x[0],(x[1][0]/x[1][1]))). \
          sortByKey()

res = kind.join(rate). \
           map(lambda x: (x[1][0],(x[1][1],1))).\
           reduceByKey(lambda x, y: (x[0]+y[0],x[1]+y[1])). \
           map(lambda x : (x[0],(x[1][0]/x[1][1],x[1][1]))). \
           sortByKey() 
        #   map(lambda x : (x[1][0], x[1][1]),1)
        #   reduceByKey(lambda x, y : (x[0]+y[0],x[1]+y[1])) 

for i in res.collect():
    print(i)
print("--- %s seconds ---" % (time.time() - start_time))

