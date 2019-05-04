# helper class/functions useful for the project/exercise 
import numpy as np
import sys
from pyspark.sql.functions import col, count, countDistinct
from pyspark.sql.window import Window
from pyspark.sql.functions import count
import pyspark.sql.functions as F

def median(values):
    try:
        medianVal = np.median(values_list) 
        return int(medianVal)
    except Exception:
        return None

def distributionStats(dfRecords, partitionBy, countBy, returnCountName="total_reviews"):
    try:
        window = Window.partitionBy(partitionBy)
        dfMaxReviews = dfRecords \
            .withColumn(returnCountName, count(countBy) \
            .over(window)) \
            .drop(countBy).distinct() \
            .orderBy(F.desc(returnCountName))
        return dfMaxReviews
    except Exception:
        return None