import ast

ACCESS_KEY = "xyz"
SECRET_KEY = "xyz"
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "project987"
MOUNT_NAME = "mySource4"

# dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)
list_of_files = (dbutils.fs.ls("/mnt/%s" % MOUNT_NAME))

def comp(fileName):
  x = sc.textFile(fileName.path)
  data = (x.collect())
  newData = ((ast.literal_eval(data[0])))
  return (newData["organizations"], newData["uuid"], newData["url"], newData["title"], newData["text"], newData["published"])

new_data = (map(comp, list_of_files))
outputRdd = sc.parallelize(new_data)
outputRdd.coalesce(1,True).saveAsTextFile("/FileStore/data2.txt")