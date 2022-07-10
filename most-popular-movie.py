from pyspark.sql import SparkSession
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
