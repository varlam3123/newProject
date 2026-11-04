from Tools.demo.sortvisu import distinct
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
spark = SparkSession.Builder().appName("WindowsSparkTest").getOrCreate()

df_classes = spark.read.parquet(r'C:\MyProject\Ships\Classes')
df_ships = spark.read.parquet(r'C:\MyProject\Ships\Ships')
df_outcomes = spark.read.parquet(r'C:\MyProject\Ships\Outcomes')
df_battles = spark.read.parquet(r'C:\MyProject\Ships\Battles')

def ex1():
    return df_ships.join(df_classes, df_ships['class'] == df_classes['class']) \
        .select(df_ships['class'],df_ships.name, df_classes.country) \
        .filter(df_classes.numGuns >= 10)

def ex2():
    return df_classes.filter(F.col('bore') >= 16).select(F.col('class'), F.col('country'))

def ex3():
    t1 = df_classes.join(df_ships, on='class').select(df_ships.name, df_classes.country, df_classes.bore)
    t2 = df_classes.join(df_outcomes, on=df_classes['class'] == df_outcomes['ship']) \
        .select(df_outcomes.ship, df_classes.country, df_classes.bore)
    return t1.union(t2).distinct() \
        .withColumn('bore', F.col('bore') ** 3 / 2) \
        .groupBy('country') \
        .agg(F.round(F.avg('bore'),2).alias('avgBore'))


def ex4():
    return df_outcomes.filter((F.col('battle') == 'North Atlantic') & (F.col('result') == 'sunk')) \
        .select(df_outcomes.ship).distinct()

def ex5():
    return df_ships.join(df_classes, on='class') \
        .filter((F.col('launched') >= 1922) & (F.col('displacement') > 35000)) \
        .select(F.col('ships'))

def ex6():
    return df_ships.select(F.col('name'), F.col('class')) \
            .union(df_classes.select(F.col('class'), F.col('class'))) \
            .select(F.col('class')).distinct()


def ex7():
    t1 = df_outcomes.join(df_classes, on=df_outcomes['ship'] == df_classes['class'], how='leftsemi') \
        .select(F.col('ship'), F.col('ship'))
    allShips = df_ships.select(F.col('name'), F.col('class')) \
                .union(t1) \
                .distinct()
    return allShips.groupBy('class') \
        .count().filter(F.col('count') == 1) \
        .select(F.col('class'))


def ex8():
    t1 = df_classes.filter(F.col('type') == 'bb').select(F.col('country'))
    t2 = df_classes.filter(F.col('type') == 'bc').select(F.col('country'))
    return t1.intersect(t2)


def ex9():
    t1 = df_outcomes.join(df_battles, on=df_outcomes.battle == df_battles.name) \
        .filter(F.col('result') == 'damaged') \
        .select(F.col('ship').alias('dShip'), F.col('date').alias('dDate'))

    t2 = df_outcomes.join(df_battles, on=df_outcomes.battle == df_battles.name)
    return t1.join(t2, on=(t1.dShip == t2.ship) & (t1.dDate < t2.date)) \
        .select(F.col('ship')).distinct()


def ex10():
    return df_outcomes.filter(F.col('result') == 'sunk') \
            .select(F.col('ship'), F.col('battle'))

def ex11():
    t1 = df_ships.select(F.col('launched')).distinct()
    return df_battles.withColumn('date', F.year('date')) \
        .join(t1, on=F.col('date') == t1.launched, how='leftanti')

def ex12():
    all_ships = df_ships.select(F.col('name')) \
        .union(df_outcomes.select(F.col('ship')))\
            .distinct()
    return all_ships.filter(F.col('name').rlike(r'^R'))

def ex13():
    all_ships = df_ships.select(F.col('name')) \
        .union(df_outcomes.select(F.col('ship'))) \
        .distinct()
    return all_ships.filter(F.size(F.split(F.col('name'), ' ')) == 3)

def ex14():
    return df_outcomes.filter(F.col('battle') == 'Guadalcanal') \
        .join(df_ships, on=(F.col('ship') == df_ships.name) | (F.col('ship') == df_ships['class']), how='left') \
        .join(df_classes, on='class', how='left') \
        .select(df_outcomes.ship, df_classes.displacement, df_classes.numGuns) \
        .distinct()

def ex15():
    t1 = df_outcomes.join(df_classes, on=F.col('ship') == df_classes['class']) \
        .select(F.col('ship'), F.col('country'), F.col('result'))
    t2 = df_ships.join(df_classes, on='class') \
        .select(F.col('name'), F.col('country')).withColumn('q', F.lit(None))
    res = t1.union(t2).distinct()
    return res.groupBy('country').agg(
        F.count('*').alias('cnt_all'),
        F.count('result').alias('cnt_res')) \
        .filter(F.col('cnt_res') == F.col('cnt_all')) \
        .select(F.col('country'))

def ex16():
    return df_outcomes.filter(F.col('result') == 'sunk') \
        .join(df_ships, on=(df_outcomes.ship == df_ships.name)) \
        .select(F.col('class')) \
        .union(df_classes.join(df_outcomes.filter(F.col('result') == 'sunk'),
                               on=df_classes['class'] == df_outcomes.ship) \
               .select(F.col('class'))) \
        .distinct()

def ex17():
    return df_outcomes.join(df_classes, on=F.col('ship') == df_classes['class']) \
        .select(F.col('ship'), F.col('bore')) \
        .union(df_ships.join(df_classes, on='class').select(F.col('name'), F.col('bore'))) \
        .filter(F.col('bore') == 16) \
        .select(F.col('ship')).distinct()



def ex18():
    return df_ships.filter(F.col('class') == 'Kongo') \
            .join(df_outcomes, on=df_ships.name == df_outcomes.ship) \
            .select(F.col('battle')).distinct()


def ex19():
    all_ships = df_outcomes.join(df_classes, on=F.col('ship') == df_classes['class']) \
                .select(F.col('ship'), F.col('numGuns'), F.col('displacement')) \
                .union(df_ships.join(df_classes, on='class') \
                       .select(F.col('name'), F.col('numGuns'), F.col('displacement'))) \
                .distinct()
    WindowSpec = Window.partitionBy('displacement').orderBy(F.col('numGuns').desc())
    return all_ships.withColumn('ranks', F.dense_rank().over(WindowSpec)) \
            .filter(F.col('ranks') == 1).select(F.col('ship'))


def ex20():
    return df_classes.filter(F.col('type') == 'bb') \
        .withColumn('numGuns', F.round(F.col('numGuns'),2)) \
        .select(F.round(F.avg(F.col('numGuns')),2).alias('avgGuns'))

def ex21():
    all_ships = df_outcomes.join(df_classes, on=F.col('ship') == df_classes['class']) \
        .select(F.col('ship'), F.col('class'), F.col('numGuns'), F.col('type')) \
        .union(df_ships.join(df_classes, on='class') \
            .select(F.col('name'), F.col('class'), F.col('numGuns'), F.col('type'))) \
        .distinct()
    return all_ships \
        .filter(F.col('type') == 'bb').withColumn('numGuns', F.round(F.col('numGuns'), 2)) \
        .select(F.round(F.avg('numGuns'), 2))

def ex22():
    return df_classes.join(df_ships, on='class', how='left') \
            .groupBy(df_classes['class']).min('launched')

def ex23():
    t1 = df_classes.select(F.col('class')).distinct()
    t2 = df_outcomes.join(df_classes, on=df_classes['class'] == df_outcomes.ship) \
        .select(F.col('ship'), F.col('class'), F.col('result')) \
        .union(df_ships.join(df_outcomes, on=df_ships.name == df_outcomes.ship) \
               .select(F.col('name'), F.col('class'), F.col('result'))) \
        .distinct() \
        .filter(F.col('result') == 'sunk')
    return t1.join(t2, on='class', how='left') \
            .groupBy(t1['class']).agg(
        F.count(t2.ship).alias('cnt')
    )
def ex24():
    als = df_classes.join(df_outcomes, on=df_outcomes.ship == df_classes['class'], how='left') \
        .select(F.col('ship'), F.col('class'), F.col('result')) \
        .union(df_ships.join(df_outcomes, on=df_ships.name == df_outcomes.ship, how='left')
                .join(df_classes, on='class').select(F.col('name'), df_ships['class'], F.col('result'))) \
        .distinct()
    return als.filter(F.col('result') == 'sunk') \
        .groupBy('class').agg(F.count('result').alias('cnt')) \
        .join(als.groupBy('class').count().filter(F.col('count') >= 3),
              on='class', how='leftsemi')


def ex25():
    t1 = df_outcomes.join(df_classes, on=F.col('ship') == df_classes['class']) \
        .select(F.col('ship'), F.col('country'), F.col('battle'))
    t2 = df_ships.join(df_outcomes, on=df_ships.name == df_outcomes.ship).join(df_classes, on='class') \
        .select(F.col('name'), F.col('country'), F.col('battle'))
    res = t1.union(t2).distinct()
    return res.groupBy('battle', 'country').agg(F.count('*').alias('cnt')).filter(F.col('cnt') >= 2) \
            .select(F.col('battle')).distinct()

def ex26():
    cAndb = df_classes.select(F.col('country')).distinct().join(df_battles.select(F.col('name').alias('battle')).distinct(), how='cross')
    allShips = df_classes.join(df_outcomes, on=df_outcomes.ship == df_classes['class']) \
        .select(F.col('ship'), F.col('class'), F.col('country'), F.col('battle')) \
        .union(df_ships.join(df_outcomes, on=df_ships.name == df_outcomes.ship)
                .join(df_classes, on='class').select(F.col('name'), df_ships['class'], F.col('country'), F.col('battle'))) \
        .distinct()
    return cAndb.join(allShips, on=['country','battle'], how='left')\
        .groupBy(cAndb.country, cAndb.battle).agg(
        F.count('ship').alias('count')) \
        .filter(F.col('count') == 0).select(F.col('country'), F.col('battle'))

# ----------------------------- WINDOW -----------------------------------
# Задача 1. Порядковый номер кораблей.
# Пронумеруйте все корабли в таблице ships внутри каждого класса по возрастанию года спуска на воду (launched).
# Если год совпадает или равен null, сортируйте по имени корабля.

def w1():
    windowSpec = Window.partitionBy('class').orderBy(F.col('launched'), F.col('name'))
    return df_ships.withColumn('ranks', F.row_number().over(windowSpec)) \


# • Задача 2. Самый старый корабль в классе
# Для каждого корабля выведите год спуска самого первого (раннего) корабля в его классе.

def w2():
    windowSpec = Window.partitionBy('class').orderBy(F.col('launched'))
    return df_ships.withColumn('first_value', F.first_value('launched').over(windowSpec))


# • Задача 3. Разница в годах спуска
# Для каждого корабля найдите, на сколько лет позже он был спущен на воду по сравнению с предыдущим кораблем в том же классе.
# • (сортировать по launched и name).

def w3():
    windowSpec = Window.partitionBy('class').orderBy(F.col('launched'), F.col('name'))
    return df_ships.withColumn('difference', (F.col('launched') - F.lag(F.col('launched')).over(windowSpec)))

# Задача 4. Последнее сражение для каждого корабля
# Используя таблицу outcomes и даты из battles,
# для каждого корабля выведите его последнее сражение по дате (включая те корабли, которые были там потоплены)

def w4():
    windowSpec = Window.partitionBy('ship').orderBy(F.col('date').desc())
    return df_outcomes.join(df_battles, on=df_outcomes.battle == df_battles.name, how='left') \
    .withColumn('last_fight', F.dense_rank().over(windowSpec)) \
    .select(F.col('ship'), F.col('battle'), F.col('result')).filter(F.col('last_fight') == 1)

# Задача 5. Скользящее среднее водоизмещения
# Для каждого класса боевых кораблей (type = 'bb')
# рассчитайте скользящее среднее водоизмещения (displacement) среди трех классов:
# текущего, одного предыдущего и одного следующего (если сортировать классы по алфавиту)

def w5():
    windowSpec = Window.orderBy(F.col('class')).rowsBetween(-1, 1)
    return df_classes.filter(F.col('type') == 'bb') \
    .withColumn('moving_average', F.avg('displacement').over(windowSpec)) \
    .select(F.col('class'), F.col('displacement'), F.col('moving_average'))


# • Задача 6. Ранжирование стран по количеству пушек
# Выведите список всех классов кораблей.
# Добавьте столбец с рангом класса внутри его страны по количеству пушек (numGuns) от большего к меньшему.
# Используйте функцию, которая не оставляет пропусков в рангах при одинаковом количестве пушек.

def w6():
    windowSpec = Window.partitionBy(F.col('country')).orderBy(F.col('numGuns').desc())
    return df_classes.withColumn('ranks', F.dense_rank().over(windowSpec)) \
    .select(F.col('class'), F.col('country'), F.col('numGuns'), F.col('ranks'))

# Задача 7. Доля водоизмещения класса от максимума страны
# Для каждого класса найдите, какую долю (в процентах)
# составляет его водоизмещение от максимального водоизмещения среди всех классов этой же страны.

def w7():
    windowSpec = Window.partitionBy(F.col('country'))
    return df_classes.withColumn('percOf', F.col('displacement') / F.max(F.col('displacement')).over(windowSpec) * 100.0) \
    .select(F.col('class'), F.col('country'), F.col('displacement'), F.col('percOf'))

# Задача 8. Выявление серий побед/участий кораблей
# Для кораблей, участвовавших более чем в одном сражении,
# выведите список битв, хронологически предыдущий результат корабля в ведении боевых действий и его текущий результат.

def w8():
    windowSpec = Window.partitionBy(F.col('ship')).orderBy(F.col('date'))
    return df_outcomes.join(df_battles, on=df_battles.name == df_outcomes.battle) \
    .join(df_outcomes.groupBy('ship').count().filter(F.col('count') >= 2), on='ship', how='leftsemi') \
    .withColumn('lag_res', F.lag('result').over(windowSpec)) \
    .withColumn('now_res', F.last_value('result').over(windowSpec)) \
    .select(F.col('ship'), F.col('battle'), F.col('lag_res'), F.col('now_res'))



# • Задача 9. Поиск "главных" кораблей без явной связи
# По правилам sql-ex, головным считается корабль, чье имя совпадает с именем класса.
# Используя окно, найдите для каждого корабля имя головного корабля его класса.
# Если головной корабль вообще отсутствует в таблице ships (но класс существует в classes), выведите No Head Ship.

def w9():
    windowSped = Window.partitionBy(F.col('ship'))

# Задача 10. Хронология живучести кораблей по странам
# Постройте непрерывную хронологическую ленту всех сражений.
# Для каждого сражения рассчитайте кумулятивное
# (нарастающим итогом с начала времен до даты текущего сражения включительно) количество кораблей каждой страны,
# которые к этому моменту уже были потоплены (sunk).

def w10():
    windowSpec = Window.orderBy(F.col('date'))
    return df_battles.join(df_outcomes, on= df_outcomes.battle == df_battles.name) \
    .withColumn('res', F.count('*').over(windowSpec)).orderBy(F.col('date')) \
    .select(F.col('name'), F.col('date'), F.col('country'),F.col('res'))




w8().show()