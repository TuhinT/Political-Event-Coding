import pyspark
import ast
from stanfordcorenlp import StanfordCoreNLP
from pymongo import MongoClient
import nltk
import re
from pyspark.streaming import StreamingContext
from datetime import datetime

sc = pyspark.SparkContext('local[*]')

rdd = sc.textFile("/data/data2.txt")

# dataStream = (ssc.textFileStream("/data/data2.txt"))

# (dataStream.count())
# dataStream.pprint()
# parse_1 = dataStream.flatMap(lambda x: x.split("\n"))

# print(parse_1.count())
def process(data):
    try:
        nlp = StanfordCoreNLP(r'/data/stanford-corenlp-full-2018-02-27')
        newData = ((ast.literal_eval(data)))
        client = MongoClient('192.168.99.100', 32780)

        che = client.local
        
        d = nlp.parse(newData[4])
        nlp.close()
        
        sent_text_1 = re.sub( '\s+', ' ', d ).strip()
     #     for sentence in sent_text:
    #         tokenized_text = nlp.parse(sentence)
    #         li.append(sentence)
        Metadata2 = {
            'language' : "english",
            'title' : newData[3],
            'url' : newData[2],
            'stanford' : 1,
            'content' : newData[4],
            'source' : newData[0],
            'parsed_sents' : [ sent_text_1 ],
            'date' : newData[5],
            'date_added': datetime.strptime(newData[5][:10], "%Y-%m-%d")
        }
        r = Metadata2
        result=che['test9'].insert_one(Metadata2)
    except:
        r = ""
    
#     print(result)

    return(r)

# rd = rdd.collect()


newa = rdd.map(lambda s: process(s))
print('check')
print(newa.collect())