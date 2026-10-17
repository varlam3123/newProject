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



ex1 = spark.sql("""
         select distinct maker
        from product
         where type = 'Printer'
""")

ex2 = spark.sql("""
         select distinct maker
         from product
         where type = 'Printer'
""")

ex3 = spark.sql("""
    SELECT model, ram, screen
    FROM Laptop
    WHERE price > 1000
""")

ex4 = spark.sql("""
    select model
    from Printer
    where price = (select max(price)
                   from Printer)
""")

ex5 = spark.sql("select avg(speed) as avg from PC")

ex6 =  spark.sql("""select avg(speed) avg
           from pc join product p on pc.model = p.model
           where maker = 'A'""")
