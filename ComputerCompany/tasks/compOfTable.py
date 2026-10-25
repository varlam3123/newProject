from comparator import comparator
import SQLSolution as sql
import SparkSolution as spark


print(comparator(spark.ex1(), sql.ex1()))
print(comparator(spark.ex10(), sql.ex10()))