from pyspark.sql import SparkSession
spark = SparkSession.Builder().appName("WindowsSparkTest").getOrCreate()

Classes = spark.read.parquet(r'C:\MyProject\Ships\Classes')
Ships = spark.read.parquet(r'C:\MyProject\Ships\Ships')
Outcomes = spark.read.parquet(r'C:\MyProject\Ships\Outcomes')
Battles = spark.read.parquet(r'C:\MyProject\Ships\Battles')

