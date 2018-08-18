from pyspark import SparkContext
import itertools
from collections import defaultdict
import math
import time
import sys

start_time = time.time()


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
    return (rowline[0], rowline[1])


def readratingsdata(fname):
    rowline = str(fname).split(',')
    return (rowline[0], rowline[1], rowline[2])


def convert_test_data(rowData):
    return (int(rowData[0]), int(rowData[1]))


def convert_train_data(rowData):
    return (int(rowData[0]), int(rowData[1]))


def convert_rating_data(rowData):
    return ((int(rowData[0]), int(rowData[1])), float(rowData[2]))


def convert_to_str((user, movie), ratings):
    return (str(user) + "," + str(movie) + "," + str(ratings))


def compute_difference((user, movie), (rate1, rate2)):
    return abs(rate1 - rate2)


def compute_pear_corr(user1, user2, corated, target, other):
    global avg_dict

    len_of_corated = len(corated)

    avg_co_rating_1 = sum([user1[j] for j in corated]) / len_of_corated
    avg_co_rating_2 = sum([user2[j] for j in corated]) / len_of_corated

    avg_dict[target][other] = avg_co_rating_1

    
    numerator = sum([(user1[i] - avg_co_rating_1) * (user2[i] - avg_co_rating_2) for i in corated])
    denominator = math.sqrt(sum([(user1[i] - avg_co_rating_1) ** 2 for i in corated])) * math.sqrt(
        sum([(user2[i] - avg_co_rating_2) ** 2 for i in corated]))

    if denominator == 0:
        return 0
    presult = numerator / denominator
    # print presult
    return presult


def pearson_correlation(user1, user2, active, other):
    corated = []

    for i in user1:
        if i in user2:
            corated.append(i)

    if len(corated) == 0:
        avg_dict[active][other] = 0
        return 0

    results = compute_pear_corr(user1, user2, corated, active, other)
    return results


def compute_sim_neigh(active, nNeighbors):
    nNeighbors = None
    similarities = [(pearson_correlation(user_dict_pro[active], user_dict_pro[other], active, other), other) for other
                    in user_dict_pro if active != other]

    similarities.sort(reverse=True)

    similarities = list(filter(lambda line: line[0] != 0, similarities))

    final_m = dict(similarities)
    res_final = dict((v, k) for k, v in final_m.iteritems())

    return similarities, res_final

def compute_avg(user):
        addedValue = 0
        avg=0
        for movie in user_dict_pro[user]:
            addedValue += user_dict_pro[user][movie]
            avg = addedValue / (len(user_dict_pro[user]))

        return avg

def predictive_ratings(user,movie):
        global similar
        temp1 = compute_avg(user)
        numerator = 0
        denominator = 0
        for person in testdata_users_alone:
            if person != user:
                if user_dict_pro.get(person, {}).has_key(movie):
                    ratings1 = user_dict_pro[person][movie]
                    #print "ratings1"+ ratings1
                    #print avg_dict[person][user]
                    ratings_avg = avg_dict[user][person]
                    sub = ratings1 - ratings_avg
                    if finalmodel.get(person, {}).has_key(user):
                        similar = finalmodel[person][user]
                    else:
                        similar = 0
                    final = sub * similar
                    numerator += final
                    denominator += abs(similar)
        if(denominator!=0):
            temp2 = numerator / denominator
        else:
            temp2 = 0
        prediction = temp1 + temp2
        return prediction


def compute_user_cf(ftrain,ftest):
	sc = SparkContext()
	global user_dict_pro, pathDump, avg_dict, test_users_list, testdata_users_alone,finalmodel
	
	nNeighbors = 3
	predicted_rating = defaultdict(dict)
	user_dict_pro = defaultdict(dict)
	avg_dict = defaultdict(dict)
	model = defaultdict(dict)
	finalmodel = defaultdict(dict)
	
	train_data_rdd = sc.textFile(ftrain)
	test_data_rdd = sc.textFile(ftest)
	
	header_info_train = train_data_rdd.first()
	header_info_test = test_data_rdd.first()
	
	train_1 = train_data_rdd.filter(lambda element: element != header_info_train)
	test_1 = test_data_rdd.filter(lambda element: element != header_info_test)
	
	train_users_total_1 = train_1.map(lambda line: readtraindata(line))
	test_users_1 = test_1.map(lambda line: readtestdata(line))
	
	train_users_total = train_users_total_1.map(lambda rows: convert_train_data(rows))
	test_users = test_users_1.map(lambda rows: convert_test_data(rows))
	
	test_users_list = test_users.collect()
	
	testdata_users_alone = test_users.map(lambda x: x[0]).distinct().collect()
	
	train_users = train_users_total.subtract(test_users).map(lambda (user, movie): ((user, movie), 1))
	
	ratings_1 = train_1.map(lambda line: readratingsdata(line))
	ratings_col = ratings_1.map(lambda rows: convert_rating_data(rows))
	
	train_ratings = ratings_col.join(train_users).map(lambda ((user, movie), (rating1, rating2)): (user, movie, rating1))
	
	train_ratings_list = train_ratings.collect()
	
	for (user, movie, ratings) in train_ratings_list:
		user_dict_pro[user][movie] = ratings
	
	for user in user_dict_pro:
		model[user], finalmodel[user] = compute_sim_neigh(user, nNeighbors)
	
	user_model = model
	
	for user,movie in test_users_list:
	
		rate = predictive_ratings(user,movie)
	
		if (rate >= 0.0 and rate <= 5.0):
			predicted_rating[user][movie] = rate
		else:
			print("Outlier at", user, movie, rate)
			predicted_rating[user][movie] = rate
	
	headers = [["UserId", "MovieId", "Pred_rating"]]
	header = sc.parallelize(headers)
	predicted_rdd = test_users.map(lambda (user, movie): ((user, movie), predicted_rating[user][movie]))
	
	joined_rdd = ratings_col.join(predicted_rdd)
	
	difference = joined_rdd.map(lambda ((user, movie), (rate1, rate2)): compute_difference((user, movie), (rate1, rate2)))
	
	predicted_rdd = predicted_rdd.map(lambda ((user, movie), ratings): convert_to_str((user, movie), ratings))
	
	final_rdd = header.union(predicted_rdd)
	
	end_time = time.time()
	total_time = end_time-start_time
	
	final_rdd.coalesce(1).saveAsTextFile("Prashanth_Manja_UserBasedCF6000.txt")
	btw_0_1 = difference.filter(lambda (rating): rating >= 0 and rating < 1).count()
	btw_1_2 = difference.filter(lambda (rating): rating >= 1 and rating < 2).count()
	btw_2_3 = difference.filter(lambda (rating): rating >= 2 and rating < 3).count()
	btw_3_4 = difference.filter(lambda (rating): rating >= 3 and rating < 4).count()
	btw_4_5 = difference.filter(lambda (rating): rating >= 4).count()
	MSE = difference.map(lambda row: row ** 2).mean()
	RMSE = math.sqrt(MSE);
	
	print(">=0 and <1:", btw_0_1)
	print(">=1 and <2:", btw_1_2)
	print(">=2 and <3:", btw_2_3)
	print(">=3 and <4:", btw_3_4)
	print(">=4 :", btw_4_5)
	print("RMSE :", RMSE)


	print("Total_Execution_Time_Is-->",str(total_time))

compute_user_cf(training_file,testing_file)