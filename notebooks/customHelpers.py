# helper class/functions useful for the project/exercise 
import numpy as np
import sys
from pyspark.sql.functions import col, count, countDistinct
from pyspark.sql.window import Window
from pyspark.sql.functions import count
import pyspark.sql.functions as F
from pyspark.sql import DataFrameStatFunctions as statFunc
from nltk.tokenize import sent_tokenize
from pyspark.sql.types import StructType, StructField, StringType,IntegerType, FloatType,BooleanType,DateType,ArrayType
from pyspark.ml.linalg import Vectors, VectorUDT
import tensorflow as tf
import tensorflow_hub as hub


VECTOR_SCHEMA = StructType([
    StructField("review_id", StringType(), True),
    StructField("vectors",VectorUDT(),True)])

    
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

def getTopBySentMedian(dfRecords, partitionBy="customer_id", textCol="review_body",medianColName="medianSents", n=10):
    countColName="count_sents"
    try: 
        # first - generate the sentence counts for each row
        # second - group by the partition & aggregate over group using a median calc 
        # last - order by the median sentences in desc orderand limit output to 10 rows 
        median = F.expr('percentile_approx(count_sents, 0.5)') 
        dfTop = dfRecords \
            .withColumn(countColName, CountSents(textCol)) \
            .groupBy(partitionBy).agg(median.alias(medianColName)) \
            .orderBy(F.desc(medianColName)) \
            .limit(n)
        return dfTop
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

@F.udf(returnType=ArrayType(StringType(), False))
def GenerateSentences(review_text): 
    '''
    utilise nltk tokenizer and generate individual sentences from text
    generates a ArrayType 
    '''
    # sent tokenize
    sentences = sent_tokenize(review_text)
    #return np.asarray(sentences)
    return sentences
     

@F.udf(returnType=IntegerType())
def CountSents(review_text): 
    '''
    count the number of sentences in each review
    '''
    # sent tokenize and return number of sents (count)
    return len(sent_tokenize(review_text))

def CosineDistance(vect_pair):    
    
    # Calculate the distance between two vectors givens its vectors in index ids.
    # do not calculate the distance between a vectors and itself, assign 0 dist in that case.
    if (vect_pair[0][1] != vect_pair[1][1]):
        distance = 1 - (vect_pair[0][0].dot(vect_pair[1][0])/(vect_pair[0][0].norm(2)*vect_pair[1][0].norm(2)))
    else:
        distance = 0

    return [vect_pair[0][1], vect_pair[1][1], float(distance)]

# negative embeddings
def vectorizeSents(lines):
    reviewids = list()
    sentences = []
    vectors=list()
    module_url = "https://tfhub.dev/google/universal-sentence-encoder/2"
    # module_url="../embedders"
    embed = hub.Module(module_url)
    
    # unpack the lines 
    for line in lines: 
        items = list(line)
        reviewids.append(items[0])
        sentences.append(items[1])
    
    with tf.Session() as session:
        session.run([tf.global_variables_initializer(), tf.tables_initializer()])
        sent_embeddings = session.run(embed(sentences))
    
    # convert to vector before append to list
    for embedding in sent_embeddings: 
        vectors.append(Vectors.dense(embedding))
    # build the return map rows
    newlines = zip(reviewids, vectors)
            
    return (newlines)