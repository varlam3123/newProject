import SparkSolution as spark
import SQLSolution as sql
from comparator import comparator

print(comparator(spark.w10(), sql.w10()))