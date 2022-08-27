from pyspark import SparkContext
from pyspark.streaming import StreamingContext,DStream
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row
import json
from collections import OrderedDict
import re
import ast

config=SparkContext("local[2]","NetworkWordCount")
bcc=StreamingContext(config,1)
#sqc=SQLContext(config)
spark=SparkSession.builder.appName('Crime').getOrCreate()



def jsonToDataframe(rdd):

	if not rdd.isEmpty():
		col=["Dates","Category","Description","DayOfWeek","District","Resolution","Address","X","Y"]
		'''
		col = StructType([StructField("Dates", StringType(), True),\
                                      StructField("Category", StringType(), True),\
                                      StructField("Description", StringType(), True),\
                                      StructField("DayOfWeek", StringType(), True),\
                                      StructField("District", StringType(), True),\
                                      StructField("Resolution", StringType(), True),\
                                      StructField("Address", StringType(), True),\
                                      StructField("X", StringType(), True),\
                                      StructField("Y", StringType(), True)
                                     ])
        '''
		dataf=spark.read.json(rdd)
		for row in df.rdd.toLocalIterator():
			'''
			r11=".".join(row)
			print(type(r11))
			r12=eval(r11)
			print(type(r12))

			'''
			for r11 in row:

				res=ast.literal_eval(r11)
				row11=[]
				for line in res:
					line=re.sub('\\n','',line)
					line=re.sub(r',(?=[^"]*"(?:[^"]*"[^"]*")*[^"]*$)',"",line)
					line=re.sub('"',"",line)
					rowList=line.split(',')
					if not "Dates" in rowList:
						row11.append(rowList)
			#newrdd = conf.parallelize(r12)
			dataf1=spark.createDataFrame(row1,schema=cols)
			dataf1.show(5)



lines1 = bcc.socketTextStream("localhost", 6100)
lines1.foreachRDD(lambda rdd: jsonToDf(rdd))
#print('##################################################### printing words..')
#print(lines1)
#print(type(lines1))
bcc.start()
bcc.awaitTermination()
bcc.stop(stopSparkContext=False)





	


