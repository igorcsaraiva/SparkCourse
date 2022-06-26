from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster('local').setAppName('minumum-temperature')
sc = SparkContext(conf=conf)

rdd = sc.textFile('/home/igor/Documentos/SparkCourse/1800.csv')

#text = rdd.map(lambda x: x.split(',')).map(lambda x: (x[0],x[1],x[2],(float(x[3])*0.1*(9.0/5.0)+32.0))).filter(lambda x: x[2] == 'TMIN').min(lambda x: x[3])

text = rdd.map(lambda x: (x.split(',')[0],x.split(',')[1],x.split(',')[2],int(x.split(',')[3])*0.1*(9.0/5.0)+32.0)).filter(lambda x: "TMIN" in x).groupBy(lambda x: x[0]).map(lambda x: (x[0],list(x[1])))
text.foreach(lambda x: print(min(x[1])))
#print(text)

