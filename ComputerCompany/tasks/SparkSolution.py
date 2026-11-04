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
    return table.join(df_product, on=['maker', 'type'], how='left') \
        .groupBy(table.maker, table.type).agg(F.count('model').alias('cnt_type')) \
    .withColumn('allCnt', F.sum('cnt_type').over(Window.partitionBy('maker'))) \
    .withColumn('res', F.round(F.col('cnt_type') / F.col('allCnt') * 100, 2)) \
    .select(F.col('maker'), F.col('type'), F.col('res'))



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


# ----------------------------- WINDOW -----------------------------------

# • Задача 1. Порядковый номер моделей внутри производителей
# Пронумеруйте все модели в таблице Product внутри каждого типа продукции (type) по алфавиту (model).

def w1():
    windowSpec = Window.partitionBy('type').orderBy('model')
    return df_product.withColumn('row_number', F.row_number().over(windowSpec))





# • Задача 2. Самый дорогой ПК у производителя
# Для каждого ПК выведите максимальную цену среди всех ПК того же производителя.

def w2():
    windowSpec = Window.partitionBy('maker')
    return df_product.join(df_pc, on='model').withColumn('max_price', F.max('price').over(windowSpec)) \
    .select(F.col('maker'), F.col('model'), F.col('price'), F.col('max_price'))



# • Задача 3. Разница в цене со следующей моделью ноутбука
# Отсортируйте ноутбуки по цене от дешевых к дорогим. Для каждого ноутбука посчитайте,
# на сколько он дешевле следующего по стоимости ноутбука.

def w3():
    windowSpec = Window.orderBy('price')
    return df_laptop.withColumn('diff', F.lead('price').over(windowSpec) - F.col('price')) \
    .select(F.col('model'), F.col('price'), F.col('diff'))




# • Задача 4. Позиция ноутбука в ценовом рейтинге компании
# Для каждого ноутбука определите его ранг по цене внутри его производителя (от самого дорогого к дешевому).
# Ранги не должны иметь пропусков, если цены одинаковые.

def w4():
    windowSpec = Window.partitionBy('maker').orderBy(F.col('price').desc())
    return df_product.join(df_laptop, on='model').withColumn('ranks', F.dense_rank().over(windowSpec)) \
    .select(F.col('maker'), F.col('model'), F.col('price'), F.col('ranks'))

# • Задача 5. Средняя цена принтеров в скользящем окне
# Отсортируйте все принтеры по номеру модели.
# Рассчитайте скользящую среднюю цену для каждого принтера,
# включая текущий, один предыдущий и один следующий принтер в списке.

def w5():
    windowSpec = Window.orderBy('model').rowsBetween(-1, 1)
    return df_printer.withColumn('avg_price', F.avg('price').over(windowSpec)) \
    .select('model', 'price', 'avg_price')


# • Задача 6. Разделение моделей ПК на ценовые группы
# Разбейте все доступные ПК на 4 равные по количеству элементов группы (квартили),
# отсортировав их по цене от меньшей к большей.
# Выведите номер группы для каждого ПК.

def w6():
    windowSpec = Window.orderBy('price')
    return df_pc.withColumn('sort_price', F.ntile(4).over(windowSpec)) \
    .select('model', 'price', 'sort_price')




# • Задача 7. Отклонение стоимости цветного принтера от средней
# Для каждого цветного принтера (color = 'y') найдите,
# на сколько процентов его цена отличается от средней цены всех цветных принтеров этого же производителя.
def w7():
    windowSpec = Window.partitionBy('maker')
    return df_printer.filter(F.col('color') == 'y').join(df_product, on='model') \
    .withColumn('relprice', F.abs(F.col('price') - F.avg('price').over(windowSpec)) / F.avg('price').over(windowSpec) * 100.0) \
        .select(F.col('maker'), F.col('model'), F.col('price'), F.col('relprice'))





# • Задача 8. Выявление ценовых пиков среди ноутбуков
# Выведите только те ноутбуки, цена которых строго выше, чем цена как у предыдущего,
# так и у следующего ноутбука в таблице (если отсортировать все ноутбуки по объему памяти ram по возрастанию, а затем по model).

def w8():
    windowSpec = Window.orderBy('ram', 'model')
    return df_laptop.withColumn('res', F.when((F.col('price') > F.lag('price').over(windowSpec))
                                       & (F.col('price') > F.lead('price').over(windowSpec)),1).otherwise(0)) \
    .filter(F.col('res') == 1).select('model', 'ram', 'price')




# • Задача 9. Поиск производителей с непрерывным ростом цен на ПК
# Для каждого производителя найдите модели ПК, цена которых идет строго по нарастающей по сравнению с хронологически
# (по номеру модели) предыдущим ПК этого же производителя. Если цена упала или осталась прежней — выведите флаг 0, если выросла — 1
# (для первой модели производителя выведите 1).

def w9():
    windowSpec = Window.partitionBy('maker').orderBy('model')
    return df_product.join(df_pc, on='model').withColumn('diff', F.when(F.lag('price').over(windowSpec).isNull(), 1)
                                                .when(F.col('price') > F.lag('price').over(windowSpec), 1).otherwise(0)) \
    .select(F.col('maker'), F.col('model'), F.col('price'), F.col('diff'))





# • Задача 10. Кумулятивная доля рынка по стоимости товаров
# Объедините все устройства (ПК, ноутбуки, принтеры) в общую витрину с указанием производителя, модели и цены.
# Рассчитайте нарастающим итогом (по увеличению цены) кумулятивную сумму стоимости всех товаров для каждого производителя,
# а также долю (в процентах) этой кумулятивной суммы от общей стоимости всех товаров этого производителя.