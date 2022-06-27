from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName('Spark-Sql').getOrCreate()

text = spark.sparkContext.textFile('/home/igor/Documentos/SparkCourse/fakefriends.csv')

data = text.map(lambda x: Row(Id = int(x.split(',')[0]), name = x.split(',')[1],age = int(x.split(',')[2]), numFriends = int(x.split(',')[3]) )).collect()

schemaData = spark.createDataFrame(data).cache()
schemaData.createOrReplaceTempView("data")

spark.sql("Select age, avg(numFriends) as media from data group by age order by media").show(500)
spark.stop()