from itertools import count
from pyspark import SparkConf,SparkContext
import collections
from operator import add
import time

conf = SparkConf().setMaster('local').setAppName('FakeFriends')
sc = SparkContext(conf=conf)

def age_and_friends(list):
    age = int(list.split(',')[2])
    friends = int(list.split(',')[3])
    return age,friends

# def sum_len(list):
#     lenght = len(list)
#     acum = 0
#     for value in list:
#         acum = value + acum
#     return acum,lenght


rdd = sc.textFile('/home/igor/Documentos/SparkCourse/fakefriends.csv')

age_friends = rdd.map(age_and_friends)
# age_friends = age_friends.groupByKey().mapValues(sum_len).collect()

## idade com maior media de numeros de amigos -> (63,384)
age_friends = age_friends.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])).mapValues(lambda x: x[0]/x[1])
age = age_friends.max(lambda x: x[1])
print(age)

## idade com maior numeros de amigos -> (40,4264)
# age_friends = age_friends.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])).mapValues(lambda x: x[0])
# age = age_friends.max(lambda x: x[1])
# print(age)
