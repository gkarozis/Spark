from pyspark.sql import SparkSession

from io import StringIO
import csv 
import datetime
import time

start_time = time.time()
spark = SparkSession.builder.appName("query5-rdd").getOrCreate()

sc = spark.sparkContext

movies = sc.textFile("hdfs://master:9000/Excercise/movies.csv")

movie_genres = sc.textFile("hdfs://master:9000/Excercise/movie_genres.csv").\
                  map(lambda x : (int(x.split(",")[0]),x.split(",")[1]))


rate = sc.textFile("hdfs://master:9000/Excercise/ratings.csv",100). \
          map(lambda x : (int(x.split(",")[1]),(int(x.split(",")[0]),float(x.split(",")[2]))))

movies = sc.textFile("hdfs://master:9000/Excercise/movies.csv").\
             map(lambda x : (int(x.split(",")[0]),(x.split(",")[1],x.split(",")[7])))
'''
mostp = movies.join(rate). \
             map(lambda x: (x[0],(x[1][0],1))). \
             reduceByKey(lambda x, y: (x[0],x[1]+y[1]))
'''
def get_res2(x1,y1,x2,y2,z1,z2):
    if x1<x2:
       return z1
    elif x2<x1:
       return z2
    else:
       if float(y1)>float(y2): 
          return z1
       else:
          return z2

def get_res(x1,y1,x2,y2,z1,z2):
    if x1>x2:
       return z1
    elif x2>x1:
       return z2
    else:
        if float(y1)>float(y2):
           return z1
        else:
           return z2

new = rate.join(movie_genres).join(movies).\
          sortByKey().\
       map(lambda x : ((x[1][0][1],x[1][0][0][0]),((x[1][0][0][1],x[1][0][0][1]),(x[1][1],x[1][1]),1))).\
       reduceByKey( lambda x,y : ((x[0][0] if x[0][0]>y[0][0] else y[0][0],x[0][1] if x[0][1]<y[0][1] else y[0][1]),(get_res(x[0][0],x[1][0][1],y[0][0],y[1][0][1],x[1][0],y[1][0]),get_res2(x[0][1],x[1][1][1],y[0][1],y[1][1][1],x[1][1],y[1][1])),x[2]+y[2])).\
       map(lambda x : (x[0][0],(x[0][1],x[1][2],x[1][1][0][0],x[1][0][0],x[1][1][1][0],x[1][0][1]))).\
       reduceByKey(lambda x, y : x if x[1]>y[1] else y).\
       sortByKey() 




for i in new.collect():
   print(i)
print("--- %s seconds ---" % (time.time() - start_time))
