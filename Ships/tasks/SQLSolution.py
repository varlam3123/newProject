from pyspark.sql import SparkSession
spark = SparkSession.Builder().appName("WindowsSparkTest").getOrCreate()

Classes = spark.read.parquet(r'C:\MyProject\Ships\Classes')
Ships = spark.read.parquet(r'C:\MyProject\Ships\Ships')
Outcomes = spark.read.parquet(r'C:\MyProject\Ships\Outcomes')
Battles = spark.read.parquet(r'C:\MyProject\Ships\Battles')

Classes.createOrReplaceTempView("Classes")
Ships.createOrReplaceTempView("Ships")
Outcomes.createOrReplaceTempView("Outcomes")
Battles.createOrReplaceTempView("Battles")



ex1 = spark.sql(""" SELECT Classes.class, name, country
FROM Classes JOIN Ships ON Classes.class = Ships.class
WHERE numGuns >= 10
""")

ex2 = spark.sql(""" select hd
from PC 
group by hd
having count(*) >= 2
""")