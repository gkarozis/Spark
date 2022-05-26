from pyspark.sql import SparkSession
import time

start_time = time.time()
spark = SparkSession.builder.appName("query2-rdd").getOrCreate()

sc = spark.sparkContext

res = sc.textFile("hdfs://master:9000/Excercise/ratings.csv",100). \
      map(lambda x : (int(x.split(",")[0]),(float(x.split(",")[2]),1))). \
      reduceByKey(lambda x , y : (x[0]+y[0],x[1]+y[1])). \
      map(lambda x : (x[0],x[1][0]/x[1][1])). \
      sortByKey(). \
      filter(lambda x : not(x[1]<3)). \
      count()
#      count(x[0])
######PARAKATW EINAI O KWDIKAS GIA TO DATASET XWRIS TO FILTRO TO MESOU OROU####

res2 = sc.textFile("hdfs://master:9000/Excercise/ratings.csv",100). \
      map(lambda x : (int(x.split(",")[0]),(float(x.split(",")[2]),1))). \
      reduceByKey(lambda x, y : (x[0]+y[0],x[1]+y[1])). \
      map(lambda x : (x[0],x[1][0]/x[1][1])). \
      sortByKey(). \
      count()

print(res)
print(res2)

print("--- %s seconds ---" % (time.time() - start_time))
####BGALE TA SXOLIA GIA NA SOU#################
####TYPWSEI TO DATASET XWRIS H ME TO FILTRO#### 
'''
for i in res.collect():
   print(i)
'''
