from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.window import Window


spark = SparkSession.Builder().appName("WindowsSparkTest").getOrCreate()


df_pc = spark.read.parquet(r'C:\MyProject\ComputerCompany\PC')
df_laptop = spark.read.parquet(r'C:\MyProject\ComputerCompany\Laptop')
df_product = spark.read.parquet(r'C:\MyProject\ComputerCompany\Product')
df_printer = spark.read.parquet(r'C:\MyProject\ComputerCompany\Printer')


ex1 = df_pc.select(col('model'), col('speed'), col('cd')).filter(col('price') < 500)

ex2 = df_product.select(col('maker')).filter(col('type') == 'Printer').distinct()

ex3 = df_laptop.select(col('model'), col('ram'), col('screen')).filter(col('price') > 1000)

ex4 = df_printer.withColumn('rank', dense_rank() \
    .over(Window.orderBy(col('price').desc()))) \
    .filter(col('rank') == 1) \
    .select(col('model'))


ex5 = df_product.select(avg('speed').alias('avg'))

ex6 = df_pc.join(df_product, df_pc.model == df_product.model) \
     .filter(col('maker') == 'A') \
     .select(avg('speed').alias('avg'))