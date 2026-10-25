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

