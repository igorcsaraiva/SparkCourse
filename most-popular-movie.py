from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType,StructField,IntegerType,FloatType 


spark = SparkSession.builder.appName('most-popular-movie').getOrCreate()

schema = StructType([StructField("Id",IntegerType(),False),
                    StructField("IdFilme",IntegerType()),
                    StructField("Estrelas",IntegerType()),
                    StructField("Horario",IntegerType())])

df_table = spark.read.option("sep", "\t").schema(schema).csv('/home/igor/Documentos/SparkCourse/ml-100k/u.data')

df_table.registerTempTable("filmes")

df = spark.sql("Select IdFilme, count(IdFilme) as MaisVotado from filmes where Estrelas = 5 group by IdFilme order by 2 desc")

df.show()


spark_item = spark.sparkContext.textFile('/home/igor/Documentos/SparkCourse/ml-100k/u.item')

spark_item = spark_item.map(lambda x: (x.split('|')[0],x.split('|')[1])).collect()

name_films = spark.sparkContext.broadcast(spark_item)

def lookup_name(id):
    return name_films.value[id - 1][1]

lookup_name_udf = func.udf(lookup_name)

df = df.withColumn('NomeFilme', lookup_name_udf(func.col('IdFilme')))

df.show()

