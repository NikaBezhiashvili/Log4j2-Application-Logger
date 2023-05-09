from pyspark.sql import *
import os
import sys
from logger import Log4J

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


spark = SparkSession.builder \
        .appName("Hello Spark") \
        .master('local[3]') \
        .getOrCreate()

logger = Log4J(spark)

logger.info('Starting HelloSpark')

logger.info('Finished HelloSpark')

spark.stop()