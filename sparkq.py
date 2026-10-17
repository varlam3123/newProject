# import os
# import sys
#
# # Передаем путь до текущего интерпретатора Python из .venv в окружение Spark
# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
#
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
#
#
# spark = SparkSession.builder \
#     .appName("WindowsSparkTest") \
#     .master("local[*]") \
#     .getOrCreate()
#
#
# product_data = [
#     ('A', '1232', 'PC'),
#     ('A', '1233', 'PC'),
#     ('A', '1276', 'Printer'),
#     ('A', '1298', 'Laptop'),
#     ('A', '1401', 'Printer'),
#     ('A', '1408', 'Printer'),
#     ('A', '1752', 'Laptop'),
#     ('B', '1121', 'PC'),
#     ('B', '1750', 'Laptop'),
#     ('C', '1321', 'Laptop'),
#     ('D', '1288', 'Printer'),
#     ('D', '1433', 'Printer'),
#     ('E', '1260', 'PC'),
#     ('E', '1434', 'Printer'),
#     ('E', '2111', 'PC'),
#     ('E', '2112', 'PC')
# ]
# product_schema = StructType([
#     StructField("maker", StringType(), True),
#     StructField('model', StringType(), True),
#     StructField("type", StringType(), True)
#     ])
# df_product = spark.createDataFrame(product_data, schema=product_schema)
#
#
#
#
# pc_data = [
#     (1, '1232', 500, 64, 5.0, '12x', 600.0),
#     (2, '1121', 750, 128, 14.0, '40x', 850.0),
#     (3, '1233', 500, 64, 5.0, '12x', 600.0),
#     (4, '1121', 600, 128, 14.0, '40x', 850.0),
#     (5, '1121', 600, 128, 8.0, '40x', 850.0),
#     (6, '1233', 750, 128, 20.0, '50x', 950.0),
#     (7, '1232', 500, 32, 10.0, '12x', 400.0),
#     (8, '1232', 450, 64, 8.0, '24x', 350.0),
#     (9, '1232', 450, 32, 10.0, '24x', 350.0),
#     (10, '1260', 500, 32, 10.0, '12x', 350.0),
#     (11, '1233', 900, 128, 40.0, '40x', 980.0),
#     (12, '1233', 800, 128, 20.0, '50x', 978.0)
# ]
# pc_schema = StructType([
#     StructField("code", IntegerType(), False),
#     StructField("model", StringType(), True),
#     StructField("speed", IntegerType(), True),
#     StructField("ram", IntegerType(), True),
#     StructField("hd", DoubleType(), True),
#     StructField("cd", StringType(), True),
#     StructField("price", DoubleType(), True),
#     ])
# df_pc = spark.createDataFrame(pc_data, schema=pc_schema)
#
#
#
# laptop_data = [
#     (1, '1298', 350, 32, 4.0, 11, 700.0),
#     (2, '1321', 500, 64, 8.0, 12, 970.0),
#     (3, '1750', 750, 128, 12.0, 14, 1200.0),
#     (4, '1298', 600, 64, 10.0, 14, 1050.0),
#     (5, '1752', 750, 128, 10.0, 14, 1150.0),
#     (6, '1298', 450, 64, 10.0, 14, 950.0)
# ]
# laptop_schema = StructType([
#     StructField("code", IntegerType(), False),
#     StructField("model", StringType(), True),
#     StructField("speed", IntegerType(), True),
#     StructField("ram", IntegerType(), True),
#     StructField("hd", DoubleType(), True),
#     StructField("screen", IntegerType(), True),
#     StructField("price", DoubleType(), True),
#     ])
# df_laptop = spark.createDataFrame(laptop_data, schema=laptop_schema)
#
#
#
# printer_data = [
#     (1, '1276', 'n', 'Laser', 400.0),
#     (2, '1433', 'y', 'Jet', 270.0),
#     (3, '1434', 'y', 'Jet', 290.0),
#     (4, '1401', 'n', 'Matrix', 150.0),
#     (5, '1408', 'n', 'Matrix', 270.0),
#     (6, '1288', 'n', 'Laser', 400.0)
# ]
# printer_schema = StructType([
#     StructField("code", IntegerType(), False),
#     StructField("model", StringType(), True),
#     StructField("color", StringType(), True),
#     StructField("type", StringType(), True),
#     StructField("price", DoubleType(), True),
# ])
# df_printer = spark.createDataFrame(printer_data, schema=printer_schema)






# ----------------------------------SHIPS------------------------------------------------
# classes_schema = StructType([
#     StructField("class", StringType(), False),
#     StructField("type", StringType(), True),
#     StructField("country", StringType(), True),
#     StructField("numGuns", IntegerType(), True),
#     StructField("bore", DoubleType(), True),
#     StructField("displacement", IntegerType(), True)
# ])
#
# classes_data = [
#     ("Bismarck", "bb", "Germany", 8, 15.0, 42000),
#     ("Iowa", "bb", "USA", 9, 16.0, 46000),
#     ("Kongo", "bc", "Japan", 8, 14.0, 32000),
#     ("North Carolina", "bb", "USA", 9, 16.0, 37000),
#     ("Renown", "bc", "Gt.Britain", 6, 15.0, 32000),
#     ("Revenge", "bb", "Gt.Britain", 8, 15.0, 29000),
#     ("Tennessee", "bb", "USA", 12, 14.0, 32000),
#     ("Yamato", "bb", "Japan", 9, 18.0, 65000)
# ]
#
# df_classes = spark.createDataFrame(data=classes_data, schema=classes_schema)
#
#
# ships_schema = StructType([
#     StructField("name", StringType(), False),
#     StructField("class", StringType(), False),
#     StructField("launched", IntegerType(), True)
# ])
#
# ships_data = [
#     ("California", "Tennessee", 1921),
#     ("Haruna", "Kongo", 1915),
#     ("Hiei", "Kongo", 1914),
#     ("Iowa", "Iowa", 1943),
#     ("Kirishima", "Kongo", 1915),
#     ("Kongo", "Kongo", 1913),
#     ("Missouri", "Iowa", 1944),
#     ("Musashi", "Yamato", 1942),
#     ("New Jersey", "Iowa", 1943),
#     ("North Carolina", "North Carolina", 1941),
#     ("Ramillies", "Revenge", 1917),
#     ("Renown", "Renown", 1916),
#     ("Repulse", "Renown", 1916),
#     ("Resolution", "Revenge", 1916),
#     ("Revenge", "Revenge", 1916),
#     ("Royal Oak", "Revenge", 1916),
#     ("Royal Sovereign", "Revenge", 1916),
#     ("Tennessee", "Tennessee", 1920),
#     ("Washington", "North Carolina", 1941),
#     ("Wisconsin", "Iowa", 1944),
#     ("Yamato", "Yamato", 1941)
# ]
#
# df_ships = spark.createDataFrame(data=ships_data, schema=ships_schema)
#
#
# battles_schema = StructType([
#     StructField("name", StringType(), False),
#     StructField("date", DateType(), True)
# ])
#
# battles_data = [
#     ("Guadalcanal", datetime.strptime("1942-11-15", "%Y-%m-%d").date()),
#     ("North Atlantic", datetime.strptime("1941-05-25", "%Y-%m-%d").date()),
#     ("North Cape", datetime.strptime("1943-12-26", "%Y-%m-%d").date()),
#     ("Surigao Strait", datetime.strptime("1944-10-25", "%Y-%m-%d").date())
# ]
#
# df_battles = spark.createDataFrame(data=battles_data, schema=battles_schema)
#
#
# outcomes_schema = StructType([
#     StructField("ship", StringType(), False),
#     StructField("battle", StringType(), True),
#     StructField("result", StringType(), True)
# ])
#
# outcomes_data = [
#     ("Bismarck", "North Atlantic", "sunk"),
#     ("California", "Surigao Strait", "OK"),
#     ("Duke of York", "North Cape", "OK"),
#     ("Fuso", "Surigao Strait", "sunk"),
#     ("Hood", "North Atlantic", "sunk"),
#     ("King George V", "North Atlantic", "OK"),
#     ("Kirishima", "Guadalcanal", "sunk"),
#     ("Prince of Wales", "North Atlantic", "damaged"),
#     ("Rodney", "North Atlantic", "OK"),
#     ("Scharnhorst", "North Cape", "sunk"),
#     ("South Dakota", "Guadalcanal", "damaged"),
#     ("Tennessee", "Surigao Strait", "OK"),
#     ("Washington", "Guadalcanal", "OK"),
#     ("West Virginia", "Surigao Strait", "OK"),
#     ("Yamashiro", "Surigao Strait", "sunk")
# ]
#
# df_outcomes = spark.createDataFrame(data=outcomes_data, schema=outcomes_schema)
