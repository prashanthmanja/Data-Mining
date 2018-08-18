from pyspark.sql.context import SQLContext
from pyspark import SparkContext
from pyspark.sql import Row
import sys
import os
import pandas

if(sys.argv.__len__() >1):
    input_file_path = sys.argv[1]
    output_file_path = sys.argv[2]
else:
    print("Please provide correct input_file_path")
    exit(0)

sc = SparkContext(appName='inf553')

#inputfile="ratings.csv"
#outputfile="output.csv"

## Type conversion
def string_to_float(mov_rate):
    return(int(mov_rate[0]),float(mov_rate[1]))
	
def readline(fname):
    rowline = str(fname).split(',')   
    return (rowline[1], rowline[2])

## spark transformations to perform average for each movie	
def Compute_Average(rinput,output):

## ratings data
    ### Craeting ratings-rdd
    my_RDD_strings = sc.textFile(rinput+'/'+'ratings.csv') 
    data = my_RDD_strings.map(lambda line : readline(line))

    ## Extraacting header row

    header_info = data.first()

    ### Selects all the rows except the header row
    data_mr = data.filter(lambda ratings: ratings != header_info)

    data_mr = data_mr.map(lambda ratings: string_to_float(ratings))
    data_mr_sum_count = data_mr.aggregateByKey((0,0), lambda U,s: (U[0] + s, U[1] + 1) , lambda U,V: (U[0] + V[0], U[1] + V[1]))
    ### format (tag(sum,count))
    avg_ratings = data_mr_sum_count.map(lambda (a,(b,c)): (a,float(b)/c))
    ### here a=movie,b=sum,c=count

    sorted_avg_ratings = avg_ratings.sortByKey() ## sorting by movieID in accending order

## Creating output csv file based on the dataset-folder

    if 'ml-20m' in rinput:
        result_csv = 'Prashanth_Manja_result_task1_big.csv'
    else:
        result_csv = 'Prashanth_Manja_result_task1_small.csv'

    sqlContext = SQLContext(sc)  
    data_frame = sqlContext.createDataFrame(sorted_avg_ratings)
    panda_data_frame = data_frame.toPandas()

    ## Output as csv file
    panda_data_frame.to_csv(output+'/'+result_csv, encoding = 'utf-8', header = ['movieID', 'rating_avg'], index = False)


### Function call to perform average
Compute_Average(input_file_path,output_file_path)
