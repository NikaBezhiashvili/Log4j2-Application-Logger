from pyspark.sql import *
import os
import sys
from logger import Log4J
from utils import *

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['LOG4J_CONFIGURATION'] = 'file:C:/Users/Administrator/Desktop/Data/HelloSpark/log4j2.properties'



spark = SparkSession.builder \
        .appName("Hello Spark") \
        .master('local[3]') \
        .getOrCreate()

logger = Log4J(spark)

logger.info('Starting HelloSpark')

read_dataframe(spark, "datafiles/dataset.csv").show()

logger.info('Finished HelloSpark')

spark.stop()
