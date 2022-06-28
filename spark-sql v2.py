from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark-Sql').getOrCreate()

data = spark.read.option("header","true").option("inferSchema","true").csv('/home/igor/Documentos/SparkCourse/fakefriends.csv')

data.printSchema()

data.groupBy("age").avg("friends").orderBy("avg(friends)").show(500)

spark.stop()