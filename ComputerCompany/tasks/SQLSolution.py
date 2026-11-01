from pyspark.sql import SparkSession
spark = SparkSession.Builder().appName("WindowsSparkTest").getOrCreate()
df_pc = spark.read.parquet(r'C:\MyProject\ComputerCompany\PC')
df_laptop = spark.read.parquet(r'C:\MyProject\ComputerCompany\Laptop')
df_product = spark.read.parquet(r'C:\MyProject\ComputerCompany\Product')
df_printer = spark.read.parquet(r'C:\MyProject\ComputerCompany\Printer')
df_pc.createOrReplaceTempView("PC")
df_laptop.createOrReplaceTempView("Laptop")
df_product.createOrReplaceTempView("Product")
df_printer.createOrReplaceTempView("Printer")



def ex1():
    return spark.sql("""
         select model, speed, cd
         from PC
         where price < 500
""")

def ex2():
    return spark.sql("""
         select distibct maker
         from Product
         where type == 'Printer'
""")

def ex3():
    return spark.sql("""
    SELECT model, ram, screen
    FROM Laptop
    WHERE price > 1000
""")

def ex4():
    return spark.sql("""
    select model
    from Printer
    where price = (select max(price)
                   from Printer)
""")

def ex5():
    return spark.sql("select avg(speed) as avg from PC")

def ex6():
    return spark.sql("""select avg(speed) avg
           from PC join product p on pc.model = p.model
           where maker = 'A'""")

def ex7():
    return spark.sql("""
select hd
from PC 
group by hd
having count(*) >= 2
""")

def ex8():
    return spark.sql("""
select distinct p.type as type,p.model as model, speed
from product p join laptop l on p.model = l.model
where speed < all(select speed from pc)
""")

def ex9():
    return spark.sql("""
select distinct maker,price
from product p join printer pr on p.model = pr.model
where price = (select min(price) from printer where color = 'y') and color = 'y'
""")


def ex10():
    return spark.sql("""
    select maker, avg(screen) as avg
    from product p join laptop l on p.model = l.model
    group by maker
    """)

def ex11():
    return spark.sql("""
    select maker, max(price) price
    from product p
        join pc on p.model = pc.model
    group by maker
    """)

def ex12():
    return spark.sql("""
    select speed, avg(price) as avg
    from pc 
    where speed > 600
    group by speed
    """)

def ex13():
    return spark.sql("""
    select distinct maker 
    from product p join pc on p.model = pc.model
    where speed >= 750 
    INTERSECT
    select distinct maker 
    from product p join laptop l on p.model = l.model 
    where speed >= 750
    """)

def ex14():
    return spark.sql("""
        with general_table as (
            select model, price
                from PC
            union all
            select model, price
                from Laptop
            union all
            select model, price
                from Printer)
        select distinct model
        from general_table
        where price in (select max(price) from general_table)
    """)

def ex15():
    return spark.sql("""
        with min_ram as (
            select distinct maker
            from product p join PC on p.model = PC.model
            where RAM = (select MIN(RAM) FROM PC) AND speed = (select max(speed) from PC where RAM = (SELECT MIN(RAM) FROM PC))
        )
        
        
        SELECT DISTINCT maker
        FROM Product
        WHERE type = 'Printer'
        intersect
        select maker
        from min_ram
    """)

def ex16():
    return spark.sql("""
     with ppc as (select price
         from product p join pc on p.model = pc.model
         where maker = 'A'
         union all
         select price
         from product p join laptop l on p.model = l.model
         where maker = 'A'
     )

     select avg(price)
     from ppc
""")


def ex17():
    return spark.sql("""
        select maker, avg(hd) avg
        from product p join pc on p.model = pc.model
        where maker in (select distinct maker from product where type = 'Printer')
        group by maker
    """)

def ex18():
    return spark.sql("""
        with asd as (
            select maker
            from product 
            group by maker
            having count(*) = 1
            )
    
        select count(*) c
        from asd
        """)

def ex19():
    return spark.sql("""
        Select model, type
        from product
        where model NOT LIKE '%[^0-9]%' or model not LIKE '%[^A-Za-z]%'
 """)


def ex20():
    return spark.sql(""" 
    select maker, type 
    from product 
    group by maker, type
    having count(*) > 1 
    and maker in (select maker
    from product 
    group by maker
    having count(distinct type) = 1)
""")


def ex21():
    return spark.sql(""" 
    with asd as (
        select maker, price
        from pc left join product p on p.model = pc.model
        union all
        select maker, price
        from laptop l left join product p on p.model = l.model
        union all
        select maker, price
        from printer pr left join product p on p.model = pr.model
    )
    
    select maker, case when count(price) != count(*) then NULL else MAX(price) end price
    from asd
    group by maker
""")

def ex22():
    return spark.sql(""" with alltype as (
select *
from (select distinct maker from product) a cross join (select distinct type from product) b 
)

select at.maker, at.type, cast(count(p.maker) * 100.0 / b.qty as decimal(10,2)) qty
from alltype at left join product p on at.maker = p.maker and at.type = p.type join (select maker, count(*) qty from product group by maker) b on b.maker = at.maker
group by at.maker, at.type, b.qty
""")


def ex23():
    return spark.sql(""" select maker
from (select * from product where type = 'PC')p full join pc on p.model = pc.model
group by maker
having count(pc.model) = count(*)
""")

def ex24():
    return spark.sql(""" select maker, max(l.price) lpr, max(pc.price) pcp, max(pr.price) prp
from product p left join laptop l on p.model = l.model left join pc on p.model = pc.model left join printer pr on p.model = pr.model
group by maker 
having max(l.price) is not null or max(pc.price) is not null or max(pr.price) is not null
""")

