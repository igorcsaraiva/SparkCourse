from operator import add
from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster('local').setAppName('aumont-spent')
sc = SparkContext(conf=conf)

rdd = sc.textFile('/home/igor/Documentos/SparkCourse/customer-orders.csv')

text = rdd.map(lambda x: (x.split(',')[0], float(x.split(',')[2]))).reduceByKey(lambda x,y: add(x,y)).sortBy(lambda x: x[1]).collect()
print(text)