from pyspark.sql import SparkSession
spark = SparkSession.Builder().appName("WindowsSparkTest").getOrCreate()

df_classes = spark.read.parquet(r'C:\MyProject\Ships\Classes')
df_ships = spark.read.parquet(r'C:\MyProject\Ships\Ships')
df_outcomes = spark.read.parquet(r'C:\MyProject\Ships\Outcomes')
df_battles = spark.read.parquet(r'C:\MyProject\Ships\Battles')

ex1 = df_ships.join(df_classes, df_ships['class'] == df_classes['class']) \
        .select(df_ships['class'],df_ships.name, df_classes.country) \
        .filter(df_classes.numGuns >= 10)


