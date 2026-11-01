from tokenize import group

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


def ex11():
    return df_product.join(df_pc, on='model') \
    .groupBy('maker').max('price').alias('max')


def ex12():
    return df_pc.filter(col('speed') > 600) \
    .groupBy('speed').avg('price').alias('avg')

def ex13():
    table1 = df_pc.join(df_product, on='model') \
    .filter(col('speed') >= 750) \
    .select(col('maker'))
    table2 = df_laptop.join(df_product, on='model') \
    .filter(col('speed') >= 750) \
    .select(col('maker'))
    return table1.intersect(table2)


def ex14():
    t1 = df_pc.select('model', 'price')
    t2 = df_laptop.select('model', 'price')
    t3 = df_printer.select('model', 'price')
    alltable = t1.union(t2).union(t3)
    return alltable.withColumn('rank', dense_rank().over(Window.orderBy(col('price').desc()))) \
    .filter(col('rank') == 1).select('model')

def ex15():
    t1 = df_product.filter(col('type') == 'Printer').select('maker')
    windowSpec = Window.orderBy(col('ram'), col('speed').desc())
    t2 = df_pc.join(df_product, on='model') \
        .withColumn('rank', dense_rank().over(windowSpec)) \
        .filter(col('rank') == 1) \
        .select(col('maker'))
    return t2.intersect(t1)

def ex16():
    t1 = df_product.join(df_laptop, on='model') \
        .filter(col('maker') == 'A') \
        .select(col('price'))
    t2 = df_product.join(df_pc, on='model') \
        .filter(col('maker') == 'A') \
        .select(col('price'))
    return t1.union(t2).agg((F.avg(col('price')).alias('avg')))


def ex17():
    t1 = df_pc.join(df_product, on='model').select(col('maker'), col('hd'))
    t2 = df_product.filter(col('type') == 'Printer').select(col('maker')).distinct()
    return t1.join(t2, on='maker') \
        .groupBy('maker').agg(avg('hd').alias('avg'))

def ex18():
    return df_product.groupBy('maker').count() \
        .filter(col('count') == 1) \
        .select(F.count('*').alias('count'))


def ex19():
    return df_product.filter(col('model').rlike(r'^([0-9]+|[a-zA-z]+)$')) \
    .select(F.col('model') , F.col('type'))


def ex20():
    t1 = df_product.groupBy(F.col('maker'), F.col('type')) \
        .count().filter(F.col('count') > 1) \
        .select(F.col('maker'), F.col('type'))
    t2 = df_product.groupBy(F.col('maker')).agg(
        F.count_distinct('type').alias('count')).filter(F.col('count') == 1)
    return t1.join(t2, on='maker', how='leftsemi')


# def ??():
#     t1 = df_product.select(F.col('maker')).distinct().join(df_product.select(F.col('type')).distinct(), how='cross')
#     pc = df_product.join(df_pc.select(F.col('price'), F.col('model')), on='model') \
#         .groupBy(F.col('maker'), F.col('type')) \
#         .agg(F.max('price').alias('price'))
#     lp = df_product.join(df_printer.select(F.col('price'), F.col('model')), on='model') \
#         .groupBy(F.col('maker'), F.col('type')) \
#         .agg(F.max('price').alias('price'))
#     pr = df_product.join(df_laptop.select(F.col('price'), F.col('model')), on='model') \
#         .groupBy(F.col('maker'), F.col('type')) \
#         .agg(F.max('price').alias('price'))
#     res = pc.union(lp).union(pr)
#
#
#     return t1.join(res, on=(t1.maker == res.maker) & (t1.type == res.type), how='left') \
#             .select(t1.maker, t1.type, res.price) \
#             .orderBy('maker')

def ex21():
    pc = df_product.join(df_pc, on='model').select(F.col('maker'), F.col('price'))
    lp = df_product.join(df_printer, on='model').select(F.col('maker'), F.col('price'))
    pr = df_product.join(df_laptop, on='model').select(F.col('maker'), F.col('price'))
    res = pc.union(lp).union(pr)
    return res.groupBy('maker') \
            .agg(F.max('price').alias('price'))\

def ex22():
    table = df_product.select('maker').distinct().join(df_product.select('type').distinct(), how='cross')
    types = table.join(df_product, on=['maker', 'type'], how='left') \
        .groupBy(table.maker, table.type).agg(F.count('model').alias('cnt_type'))
    als = table.join(df_product, on='maker',how='left') \
        .groupBy(table.maker).count()
    return types.join(als, on='maker', how='left').withColumn('res', F.col('cnt_type') / F.col('count') * 100)



def ex23():
    return df_product.filter(F.col('type') == 'PC').join(df_pc, on='model', how='left') \
    .groupBy(F.col('maker')).agg(
        F.count('*').alias('allCnt'),
        F.count(df_pc.model).alias('pcCnt')) \
        .filter(F.col('allCnt') == F.col('pcCnt')) \
        .select('maker').distinct()


def ex24():
    allpr = df_product.join(df_printer.select(F.col('model').alias('pr_model'), F.col('price').alias('pr_price')),
            on=df_product.model == F.col('pr_model'), how='left') \
        .join(df_pc.select(F.col('model').alias('pc_model'), F.col('price').alias('pc_price')),
            on=df_product.model == F.col('pc_model'), how='left') \
        .join(df_laptop.select(F.col('model').alias('lt_model'), F.col('price').alias('lt_price')),
            on=df_product.model == F.col('lt_model'), how='left')
    return allpr.groupBy('maker').agg(
        F.max('lt_price'),
        F.max('pc_price'),
        F.max('pr_price'),
    )

