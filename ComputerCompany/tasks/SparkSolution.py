from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dense_rank, avg
from pyspark.sql.window import Window
from pyspark.sql import functions as F


spark = SparkSession.Builder().appName("WindowsSparkTest").getOrCreate()


df_pc = spark.read.parquet(r'C:\MyProject\ComputerCompany\PC')
df_laptop = spark.read.parquet(r'C:\MyProject\ComputerCompany\Laptop')
df_product = spark.read.parquet(r'C:\MyProject\ComputerCompany\Product')
df_printer = spark.read.parquet(r'C:\MyProject\ComputerCompany\Printer')


def ex1():
    return df_pc.select(col('model'), col('speed'), col('cd')).filter(col('price') < 500)

def ex2():
    return df_product.select(col('maker')).filter(col('type') == 'Printer').distinct()

def ex3():
    return df_laptop.select(col('model'), col('ram'), col('screen')).filter(col('price') > 1000)

def ex4():
    return df_printer.withColumn('rank', dense_rank() \
    .over(Window.orderBy(col('price').desc()))) \
    .filter(col('rank') == 1) \
    .select(col('model'))


def ex5():
    return df_pc.select(avg('speed').alias('avg'))

def ex6():
    return df_pc.join(df_product, df_pc.model == df_product.model) \
     .filter(col('maker') == 'A') \
     .select(avg('speed').alias('avg'))

def ex7():
    return df_pc.groupBy(col('hd')).count() \
    .filter(col('count') >= 2) \
    .select(col('hd'))

def ex8():
    return df_laptop.join(df_product, on='model') \
    .filter(col('speed') < df_pc.select(F.min('speed')).collect()[0][0]) \
        .select(F.col('type'), F.col('model'), F.col('speed')) \
        .distinct()


def ex9():
    return df_printer.join(df_product, on='model') \
    .filter(col('color') == 'y') \
    .withColumn('res', dense_rank().over(Window.orderBy('price'))) \
    .filter(col('res') == 1) \
    .select(col('maker'), col('price'))

def ex10():
    return df_product.join(df_laptop, on='model') \
    .groupBy(col('maker')).avg('screen').alias('avg')


