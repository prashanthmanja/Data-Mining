from pyspark import SparkContext
from pyspark.sql import *
import sys
from pyspark.sql import Row
from pyspark.sql.context import SQLContext
import os
import pandas

sc = SparkContext(appName='inf553')

spark_session = SparkSession(sc)

if(sys.argv.__len__() >1):
	ratings_input_file_path = sys.argv[1]
	tags_input_file_path = sys.argv[2]
	output_file_path = sys.argv[3]
else:
    print("PLease provide correct input_file_path")
    exit(0)

ratings_input_file_path = sys.argv[1]
tags_input_file_path = sys.argv[2]
output_file_path = sys.argv[3]

ratings_dataframe =spark_session.read.csv( ratings_input_file_path+'/'+'ratings.csv',header=True,inferSchema='true')

tags_dataframe = spark_session.read.csv(tags_input_file_path+'/'+'tags.csv',header=True,inferSchema='true')

ratings_df_info = ratings_dataframe.drop('userId','timestamp')

tags_df_info = tags_dataframe.drop('userId','timestamp')

rate_tag_info = tags_df_info.join(ratings_df_info,ratings_df_info['movieId'] == tags_df_info['movieId'],'inner')

rate_tag_info = rate_tag_info.drop(tags_df_info['movieId']).drop(ratings_df_info['movieId'])

rate_tag_info.createOrReplaceTempView("avgView")

rate_tag_sql=spark_session.sql('select tag,avg(rating) as Avg_rating from avgView group by tag order by tag desc')


if 'ml-20m' in ratings_input_file_path or 'ml-20m' in tags_input_file_path:
	result_csv = 'Prashanth_Manja_result_task2_big.csv'
else:
	result_csv = 'Prashanth_Manja_result_task2_small.csv'

final_result = rate_tag_sql


panda_data_frame = final_result.toPandas()

## Output as csv file
path = output_file_path+'/'+result_csv
panda_data_frame.to_csv(path, encoding = 'utf-8', header = ['tag', 'rating_avg'], index = False)

