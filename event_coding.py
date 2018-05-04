
# coding: utf-8

# In[19]:


from petrarch2 import petrarch2
from pymongo import MongoClient
import pprint


client = MongoClient('mongodb://192.168.99.100:27017')
db = client.Metadata3
collection = db.Mtable

for doc in collection.find():
    pprint.pprint(doc)
    
petr1_results = petrarch2.run_pipeline(collection.find(), write_output=False,parsed=True)
print (petr1_results)     

