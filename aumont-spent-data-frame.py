from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,FloatType 

spark = SparkSession.builder.appName('aumont-spent-data-frame').getOrCreate()

schema = StructType([StructField("Id",IntegerType(),False),StructField("CodigoProduto",IntegerType(),False),StructField("Preco",FloatType(),False)])

df = spark.read.schema(schema).csv('/home/igor/Documentos/SparkCourse/customer-orders.csv')

df = df.select("Id","Preco").groupBy("Id").sum("Preco")
df.show()