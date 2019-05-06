# helper class/functions useful for the project/exercise 
import numpy as np
import sys
from pyspark.sql.functions import col, count, countDistinct
from pyspark.sql.window import Window
from pyspark.sql.functions import count
import pyspark.sql.functions as F
from nltk.tokenize import sent_tokenize
from pyspark.sql.types import StructType, StructField, StringType,IntegerType, FloatType,BooleanType,DateType

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

def getTopBySentNumber(dfRecords, topnCol="customer_id", textCol="review_body", n=10):
    countColName="count_sents"
    try: 
        # set the window column and set it in desc order by sentence count
        window = Window.partitionBy(topnCol).orderBy(F.desc(countColName))
        # first - generater the sentence counts for each row
        # second - use of rank over window and only taking the firts ranked item for each customer/product
        # last - order by the sent count desc order and limit the final output to 10 rows only 
        dfTop = dfRecords \
            .withColumn(countColName, CountSents(textCol)) \
            .withColumn("rank", F.rank().over(window)).where(col("rank") == 1) \
            .orderBy(F.desc(countColName)) \
            .limit(n)
        return dfTop
    except Exception: 
        return None

@F.udf(returnType=BooleanType())
def FilterSentences(review_text): 
    '''
    each entry is a list of 10 items with the 9th item containing the review_body
    '''
    # sent tokenize and check the length of sentence 
    if len(sent_tokenize(review_text)) > 2: 
        return True
    return False

@F.udf(returnType=IntegerType())
def CountSents(review_text): 
    '''
    count the number of sentences in each review
    '''
    # sent tokenize and return number of sents (count)
    return len(sent_tokenize(review_text))