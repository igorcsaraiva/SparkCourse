from pyspark.sql import SparkSession,functions as func
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

spark = SparkSession.builder.appName('HeroiMaisPopular').getOrCreate()

schema = StructType([StructField("Id",IntegerType()),StructField("Nome",StringType())])

df_name = spark.read.schema(schema).option("sep", ' ').csv('/home/igor/Documentos/SparkCourse/Marvel+Names')

df_name.show()

df = spark.sparkContext.textFile('/home/igor/Documentos/SparkCourse/Marvel+Graph')

def eliminar_indice0_valor_vazio(lista: list) -> list:
    ids = []
    for value in lista:
        if(value != lista[0] and value != ''):
            ids.append(value)
    return ids
        
    

df = df.map(lambda x: x.split(' ')).map(lambda x: (x[0], len(eliminar_indice0_valor_vazio(x)))).groupByKey().mapValues(lambda x: sum(x)).max(lambda x: x[1])

print(df)
df_name = df_name.filter(df_name.Id == int(df[0])).first()

print(df_name.Nome)


