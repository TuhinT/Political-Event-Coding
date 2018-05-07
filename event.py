import pyspark
import ast
import sys
from pymongo import MongoClient
from petrarch2 import petrarch2

sys.path.append('/data')
import result_formatter

client = MongoClient('192.168.99.100', 32780)
db = client.local

collection = db['test6']

for obj in collection.find():
    petr2_results = petrarch2.run_pipeline( [ obj ], write_output=False, parsed=True)
    formatted_results = result_formatter.main(petr2_results)
    print(formatted_results)
