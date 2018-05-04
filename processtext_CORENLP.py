
# coding: utf-8

# In[3]:


from stanfordcorenlp import StanfordCoreNLP
import json
import nltk.data
import datetime
from pymongo import MongoClient

file = open("/home/jovyan/article.txt","r", encoding = 'utf-16')
filetext = file.read()
#nltk.download('punkt')
tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
sentences = tokenizer.tokenize(filetext)
#print (filetext)
props={'annotators': 'tokenize,parse,sentiment','outputFormat':'json'}
nlp = StanfordCoreNLP('http://localhost', port=9000)

jsonobject = nlp.annotate(filetext,properties=props)
parsed = json.loads(jsonobject)
prettyjson = json.dumps(parsed,indent =4, sort_keys=True)
#print (prettyjson)

i=0
client = MongoClient("mongodb://192.168.99.100:27017")
db = client.Metadata3
for val in parsed['sentences']:
    #print (val['sentimentValue'])
    language = 'english'
    title = i
    url = ""
    stanford = 1
    content = sentences[i]
    source = ""
    parsed_sents = val['parse']
    date = ""
    date_added = ""
    id = i
    #print (content,"\n")
    i+=1
    Metadata3 = {
        'language' : language,
        'title' : title,
        'url' : url,
        'stanford' : 1,
        'content' : content,
        'source' : source,
        'parsed_sents' : parsed_sents,
        'date' : date,
        'date_added' : date_added,
        
    }
    result=db.Mtable.insert_one(Metadata3)
    print (result)
    
nlp.close()

