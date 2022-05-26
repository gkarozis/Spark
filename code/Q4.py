from pyspark.sql import SparkSession
from io import StringIO
import csv
import datetime
import time

start_time = time.time()
spark = SparkSession.builder.appName("query4-rdd").getOrCreate()

sc = spark.sparkContext

def split_complex(x):
       return list(csv.reader(StringIO(x), delimiter=","))[0]

def get_year(ts):
       year=int(ts[:4])
       if year >= 2000 and year <= 2004:
          return 2000
       elif year >= 2005 and year <= 2009:
          return 2005
       elif year >= 2010 and year <= 2014:
          return 2010
       elif year >=2015 and year <=2019:
          return 2015
       else:
          return 0

movies = sc.textFile("hdfs://master:9000/Excercise/movies.csv").\
            map(lambda x: split_complex(x)).\
            filter(lambda x: not(x[3] == None or x[3] == "" or get_year(x[3]) < 2000 or x[2]=='')). \
            map(lambda x: (int(x[0]),(get_year(x[3]),len(list(x[2].split(" "))))))
          #  map(lambda x : (x[0],len(list(x[1].split(" ")))))
movie_genres = sc.textFile("hdfs://master:9000/Excercise/movie_genres.csv"). \
                  map(lambda x : (int(x.split(',')[0]),x.split(',')[1])).\
                  filter(lambda x: x[1]=="Drama")

new = movies.join(movie_genres).\
            map(lambda x : (x[1][0][0],(x[1][0][1],1))).\
            reduceByKey(lambda x, y : (x[0]+y[0],x[1]+y[1])). \
            map(lambda x : (x[0],x[1][0]/x[1][1])). \
            sortByKey()

for i in new.collect():
    print(i)
print("--- %s seconds ---" % (time.time() - start_time))
