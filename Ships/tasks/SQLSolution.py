from pyspark.sql import SparkSession

from Ships.tasks.SparkSolution import df_battles, df_outcomes

spark = SparkSession.Builder().appName("WindowsSparkTest").getOrCreate()

Classes = spark.read.parquet(r'C:\MyProject\Ships\Classes')
Ships = spark.read.parquet(r'C:\MyProject\Ships\Ships')
Outcomes = spark.read.parquet(r'C:\MyProject\Ships\Outcomes')
Battles = spark.read.parquet(r'C:\MyProject\Ships\Battles')

Classes.createOrReplaceTempView("Classes")
Ships.createOrReplaceTempView("Ships")
Outcomes.createOrReplaceTempView("Outcomes")
Battles.createOrReplaceTempView("Battles")



def ex1():
    return spark.sql(""" 
        SELECT Classes.class, name, country
        FROM Classes JOIN Ships ON Classes.class = Ships.class
        WHERE numGuns >= 10
    """)

def ex2():
    return spark.sql(""" 
    SELECT class, country
FROM Classes
WHERE bore >= 16
""")

def ex3():
    return spark.sql(""" 
        with all_ships as 
            (select name, class
            from ships
            union 
            select ship, ship
            from outcomes
            )
    select c.country, cast(avg(power(bore, 3) / 2) as numeric (6,2)) bore
    from classes c join all_ships a_s on c.class = a_s.class
    group by c.country
    """)



def ex4():
    return spark.sql(""" select ship
from Outcomes
where battle = 'North Atlantic' and result = 'sunk'
""")

def ex5():
    return spark.sql(""" select name
from ships s join classes c on s.class = c.class
where launched >= '1922' and displacement > 35000 and type = 'bb'
""")

def ex6():
    return spark.sql(""" with asd as(
select name
from Ships 
where name in (select class from classes)
union
select ship
from Outcomes
where ship in (select class from classes)
)

select distinct name
from asd
""")


def ex7():
    return spark.sql(""" with al as (
        select name, class
        from ships
        union
        select ship, ship
        from outcomes
        where ship in (select class from classes)
        )

        select class
        from al 
        group by class
        having count(*) = 1
    """)


def ex8():
    return spark.sql(""" with asd as (
        select distinct country
        from classes
        where type = 'bb'
        intersect
        select distinct country from classes
        where type = 'bc'
        )
        
        select country
        from asd
    """)


def ex9():
    return spark.sql("""
        with asd as (
            select ship, date, result
            from outcomes o join battles b on o.battle = b.name
            )
        
        select distinct ship
        from asd qwe
        where result = 'damaged'
        and exists (select * from asd where date > qwe.date and ship = qwe.ship)
    """)


def ex10():
    return spark.sql(""" select ship, battle
from outcomes
where result = 'sunk'
""")

def ex11():
    return spark.sql(""" 
        SELECT name 
        FROM battles b
        WHERE not exists (
        select *
        from ships 
        where launched = datepart(yy,b.date)
        )
""")


def ex12():
    return spark.sql(""" 
        with asd as(
        select name 
        from ships
        union
        select ship
        from outcomes
        )
        
        select distinct name
        from asd
        where name LIKE 'R%'
    """)

def ex13():
    return spark.sql("""
        with all_ships as (
            select name 
            from Ships
            union
            select ship
            from Outcomes
            )
        
        select distinct name
        from all_ships
        where name like '% % %'
""")

def ex14():
    return spark.sql(""" with asd as (
        select name, class
        from ships
        union all 
        select ship, ship
        from outcomes
        where ship not in (select name from ships)
        )
        select distinct name, displacement, numGuns
        from asd a left join classes c on a.class = c.class
        where name in (select ship from outcomes where battle = 'Guadalcanal')
""")


def ex15():
    return spark.sql("""
WITH all_ships AS (
    SELECT c.country, s.name AS ship
    FROM Classes c
    INNER JOIN Ships s ON c.class = s.class
    UNION
    SELECT c.country, o.ship AS ship
    FROM Outcomes o
    INNER JOIN Classes c ON c.class = o.ship)
    
    SELECT a.country
    FROM all_ships a
    LEFT JOIN outcomes o ON a.ship = o.ship
    GROUP BY a.country
    HAVING COUNT(DISTINCT a.ship) = COUNT(DISTINCT CASE WHEN o.result = 'sunk' THEN a.ship END)
""")


def ex16():
    return spark.sql("""
    select c.class
    from outcomes o left join ships s on o.ship = s.name join classes c on s.class = c.class or o.ship = c.class
    where result = 'sunk'
    group by c.class
""")


def ex17():
    return spark.sql(""" 
        with all_ships as (
        select name, c.class, bore
        from Ships s join classes c on s.class = c.class
        union
        select ship, ship, bore
        from outcomes o join classes c on o.ship = c.class
        )
        
        select name
        from all_ships
        where bore = '16.0'
""")


def ex18():
    return spark.sql(""" select distinct battle
from outcomes o join ships s on o.ship = s.name 
where class = 'Kongo'
""")

def ex19():
    return spark.sql(""" WITH AllShips AS (
    SELECT name, class FROM Ships
    UNION
    SELECT ship, ship FROM Outcomes
)
SELECT DISTINCT AShips.name
FROM AllShips AShips
JOIN Classes C ON AShips.class = C.class
WHERE C.numGuns = (
    SELECT MAX(C2.numGuns)
    FROM AllShips AShips2
    JOIN Classes C2 ON AShips2.class = C2.class
    WHERE C2.displacement = C.displacement
)
""")

def ex20():
    return spark.sql(""" select cast(avg(cast(numGuns as numeric(4,2))) as numeric(4,2)) avg
from classes 
where type = 'bb'
""")

def ex21():
    return spark.sql(""" with als as(
select name, cast(numguns as numeric(4,2)) numguns
from classes c join ships s on c.class = s.class
where type = 'bb'
union
select ship, cast(numguns as numeric(4,2))
from outcomes o join classes c on o.ship = c.class
where type = 'bb'
)

select cast(avg(numguns) as numeric(4,2)) avg
from als
""")

def ex22():
    return spark.sql(""" select c.class, min(launched) launched
from classes c left join ships s on c.class = s.class
group by c.class
""")

def ex23():
    return spark.sql(""" with asd as (
select name, class
from ships s join outcomes o on s.name = o.ship
where result = 'sunk'
union
select ship, ship
from outcomes 
where ship in (select class from classes) and result = 'sunk'
)


select distinct c.class, count(asd.name) qty
from classes c left join asd on c.class = asd.class
group by c.class
""")


def ex24():
    return spark.sql(""" SELECT c.class, COUNT(o.ship) AS sunk_ships
FROM Classes c
LEFT JOIN (
    SELECT name AS ship, class FROM Ships
    UNION
    SELECT ship, ship AS class FROM Outcomes WHERE ship IN (SELECT class FROM classes)
) s ON c.class = s.class
LEFT JOIN Outcomes o ON s.ship = o.ship AND o.result = 'sunk'
GROUP BY c.class
HAVING COUNT(s.ship) >= 3 and count(o.ship) > 0
""")


def ex25():
    return spark.sql(""" with asd as(
select ship, battle, country
from outcomes o join ships s on o.ship = s.name join classes c on c.class = s.class 
union
select ship, battle, country
from outcomes o join classes c on o.ship = c.class
)

select distinct battle
from asd
group by battle, country
having count(*) >= 2
""")

def ex26():
    return spark.sql(""" with all_country as (
select country, name as battle
from (select distinct country from classes) a cross join (select distinct name from battles) b
), 
ship_in_battle as (
select ship, country, battle
from outcomes o join classes c on o.ship = c.class
union 
select name, country, battle
from outcomes o join ships s on o.ship = s.name join classes c on s.class = c.class
)

select ac.country, ac.battle
from all_country ac left join ship_in_battle sib on ac.country = sib.country and ac.battle = sib.battle
group by ac.country, ac.battle
having count(sib.ship) = 0
""")
