from pyspark.sql import *
import os
import sys
from logger import Log4J
from utils import *
from pyspark.sql.functions import  desc

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['LOG4J_CONFIGURATION'] = 'file:C:/Users/Administrator/Desktop/Data/HelloSpark/log4j2.properties'



spark = SparkSession.builder \
        .appName("Hello Spark") \
        .master('local[3]') \
        .getOrCreate()

logger = Log4J(spark)

logger.info('Starting HelloSpark')

rawData = read_dataframe(spark, "datafiles/dataset.csv")
filtered_data = rawData.where(rawData['Business Account Number'] < 750) \
                        .select('Business Account Number', 'City', 'Mail Zipcode') \
                        .orderBy(desc('Business Account Number'))
filtered_data.show()

logger.info('Finished HelloSpark')

spark.stop()
