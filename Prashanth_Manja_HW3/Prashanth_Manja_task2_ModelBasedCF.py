import time
import sys
import math
from operator import add
from pyspark import SparkContext,SparkConf
from collections import defaultdict
import os, sys, operator
from pyspark.mllib.recommendation import ALS,Rating


start_time = time.time()
sc = SparkContext("local[*]",appName='inf553')

if(sys.argv.__len__() >1):
    training_file = sys.argv[1]
    testing_file = sys.argv[2]

else:
    print("Please provide correct input_file_path")
    exit(0)
	

def readtestdata(fname):
    rowline = str(fname).split(',')   
    return (rowline[0], rowline[1])

def readtraindata(fname):
    rowline = str(fname).split(',')   
    return (rowline[0], rowline[1],rowline[2])

def convert_test_data(rowData):
    return (int(rowData[0]), int(rowData[1])) 

def convert_train_data(rowData):
    return ((int(rowData[0]), int(rowData[1])),float(rowData[2])) 

def compute_model_cf(ftrain,ftest):	
	test_data_1 = sc.textFile(ftest)
	data = test_data_1.map(lambda line : readtestdata(line))
	header_info = data.first()
	test_data = data.filter(lambda ratings: ratings != header_info).map(lambda rowData : (int(rowData[0]), int(rowData[1]))).persist()
	
	
	train_data_1 = sc.textFile(ftrain).map(lambda lines : readtraindata(lines))
	header_info_train = train_data_1.first()
	train_data = train_data_1.filter(lambda rating: rating != header_info_train).map(lambda rowData:((int(rowData[0]), int(rowData[1])),float(rowData[2])))
	
	
	user_movies = train_data.map(lambda row : row[0])
	#print user_movies.collect()
	
	
	
	train_movies = user_movies.subtract(test_data).map(lambda r : (r,0))
	#print train_movies.collect()
	
	
	training_movies = train_data.join(train_movies).map(lambda r : (r[0],r[1][0]))
	
	#print training_movies.collect()
	
	
	test_movies = test_data.map(lambda r : (r,0))
	
	test_movie_rates = train_data.join(test_movies)
	
	test_movie_ratings = train_data.join(test_movie_rates).map(lambda r : (r[0],r[1][0]))
	
	
	
	ratings = training_movies.map( lambda r : Rating(r[0][0],r[0][1],r[1]))
	rank = 7
	iters = 10
	
	model = ALS.train(ratings, rank, iters)
	predictions = model.predictAll(test_data).map(lambda r: ((r[0], r[1]), r[2]))
	
	
	outliers = predictions.filter(lambda r : r[1] < 0.0 or r[1] > 5.0)
	true_Predictions = predictions.subtract(outliers)
	
	predicted_movies_rdd = predictions.map(lambda r : r[0])
	missing_movies_rdd = test_data.subtract(predicted_movies_rdd)
	
	
	
	missing_Outliers = outliers.map(lambda r : r[0]).union(missing_movies_rdd)
	
	
	userRatings = predictions.map(lambda r :  (r[0][0],r[1]))
	
	mean_Ratings = userRatings.aggregateByKey((0.0,0.0), lambda U,s: (U[0] + s, U[1] + 1) , lambda U,V: (U[0] + V[0], U[1] + V[1])).mapValues(lambda res : 1.0 * res[0] / res[1])
	
	
	user_movie_ratings = missing_Outliers.join(mean_Ratings).map(lambda r : ((r[0],r[1][0]),r[1][1]))
	
	
	predictions_union = true_Predictions.union(user_movie_ratings)
	
	ratesAndPreds = test_movie_ratings.join(predictions_union).sortByKey()
	
	
	MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
	RMSE = math.sqrt(MSE)
	
	
	difference = ratesAndPreds.map(lambda r : abs(r[1][0] - r[1][1]))
	bet_0_1 = difference.filter(lambda r : r >= 0 and r <1.0).count()
	bet_1_2 = difference.filter(lambda r : r >= 1.0 and r < 2.0).count()
	bet_2_3 = difference.filter(lambda r : r >= 2.0 and r < 3.0).count()
	bet_3_4 = difference.filter(lambda r : r >= 3.0 and r < 4.0).count()
	bet_4_5 = difference.filter(lambda r : r >= 4.0).count()
	#squared_error = ratesAndPreds.map(lambda r : math.pow((r[1][0] - r[1][1]),2)).mean()
	
	print()
	print(">=0 and <1: " ,bet_0_1)
	print(">=1 and <2: ",bet_1_2)
	print(">=2 and <3: ",bet_2_3)
	print(">=3 and <4: ",bet_3_4)
	print(">=4: ",bet_4_5)
	print("RMSE = " ,RMSE)
	
	
	headers = []
	headers = [["UserId","MovieId","Pred_rating"]]
	header = sc.parallelize(headers)
	
	resultRDD = ratesAndPreds.map(lambda r : (str(r[0][0]),str(r[0][1]),str(r[1][1]))).repartition(1)
	fileRDD = header.union(resultRDD).map(lambda r : r[0]+","+r[1]+","+r[2])
	fileRDD = fileRDD.repartition(1)
	fileRDD.saveAsTextFile("Prashanth_Manja_ModelBasedCF.txt")
	end_time = time.time()
	total_time = end_time-start_time
	print("Total_Execution_Time_Is-->",str(total_time))


#ratings_input_file ="r1.csv"
#testing_input_file ="testing_small.csv"
compute_model_cf(training_file,testing_file)