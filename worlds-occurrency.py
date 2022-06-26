from operator import add
import re
from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster('local').setAppName('Worlds-occurrency')
sc = SparkContext(conf=conf)

rdd = sc.textFile('/home/igor/Documentos/SparkCourse/Book',use_unicode=True)
def clean_string(x):
    x = str(x)
    x = x.upper()
    return re.sub(u'[^a-zA-Z0-9áéíóúÁÉÍÓÚâêîôÂÊÎÔãõÃÕçÇ: ]', '', x)
    

#text = rdd.flatMap(lambda x: x.split()).map(lambda x: (x,1)).reduceByKey(lambda x,y: add(x,y)).max(lambda x: x[1])
text = rdd.flatMap(lambda x: x.split()).map(clean_string).map(lambda x: (x,1)).reduceByKey(lambda x,y: add(x,y)).max(lambda x: x[1])
print(text)