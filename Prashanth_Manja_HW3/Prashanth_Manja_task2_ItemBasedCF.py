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
    lsh_similarity_file = sys.argv[3]

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

def similarity_data_file_1(fname):
    rowline = str(fname).split(',')
    return (int(rowline[0]), int(rowline[1]), float(rowline[2]))

def similarity_data_file_2(fname):
    rowline = str(fname).split(',')
    return (int(rowline[1]), int(rowline[0]), float(rowline[2]))


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


def convert_to_str((user, movie), ratings):
    return (str(user) + "," + str(movie) + "," + str(ratings))


def compute_difference((user, movie), (rate1, rate2)):
    return abs(rate1 - rate2)


def new_movie_predictive_ratings(user, movie):
    if user_dict_profile[user]:
        length_of_user_pro = len(user_dict_profile[user])
        avg_rating = sum([user_dict_profile[user][mov] for mov in user_dict_profile[user]]) / length_of_user_pro

        return avg_rating
    else:

        return -1

def predictive_ratings(user,movie):
    numerator = 0
    denominator = 0
    if movie in similarity_model_1.keys():
        if similarity_model_1.has_key(movie1):
            for other in similarity_model_1[movie1].keys():
                sim = similarity_model_1[movie1][other]
                if user_dict_profile[user].has_key(movie):
                    rating = user_dict_profile[user][movie]
                    numerator += sim * rating
                    denominator += abs(sim)

            if denominator != 0:
                predicted_rate = numerator / denominator
            else:
                predicted_rate = new_movie_predictive_ratings(user, movie)

            return predicted_rate

    else:
        p_rate = new_movie_predictive_ratings(user, movie)
        return p_rate


sc = SparkContext()
global user_dict_profile, pathDump, avg_dict, test_users_list, testdata_users_alone, finalmodel
pathDump = None
nNeighbors = 3
predicted_rating = defaultdict(dict)
user_dict_profile = defaultdict(dict)
movie_dict_profile = defaultdict(dict)
avg_dict = defaultdict(dict)
model = defaultdict(dict)
finalmodel = defaultdict(dict)
similarity_model_1 = defaultdict(dict)
similarity_model_2 = defaultdict(dict)
predicted_rating = defaultdict(dict)

train_data_rdd = sc.textFile(training_file)
test_data_rdd = sc.textFile(testing_file)
similarity_data_rdd = sc.textFile(lsh_similarity_file)

header_info_train = train_data_rdd.first()
header_info_test = test_data_rdd.first()

train_1 = train_data_rdd.filter(lambda element: element != header_info_train)
test_1 = test_data_rdd.filter(lambda element: element != header_info_test)

train_users_total_1 = train_1.map(lambda line: readtraindata(line))
test_users_1 = test_1.map(lambda line: readtestdata(line))

similar_data_rdd_1 = similarity_data_rdd.map(lambda line: similarity_data_file_1(line))
similar_data_rdd_2 = similarity_data_rdd.map(lambda line: similarity_data_file_2(line))

similar_data_rdd = similar_data_rdd_1
# similar_data_rdd_sim = similar_data_rdd.map(lambda x : x[2]).sort()

train_users_total = train_users_total_1.map(lambda rows: convert_train_data(rows))
test_users = test_users_1.map(lambda rows: convert_test_data(rows))

test_users_list = test_users.collect()

testdata_users_alone = test_users.map(lambda x: x[0]).distinct().collect()
testdata_movies_alone = test_users.map(lambda x: x[1]).distinct().collect()

train_users = train_users_total.subtract(test_users).map(lambda (user, movie): ((user, movie), 1))

ratings_1 = train_1.map(lambda line: readratingsdata(line))
ratings_col = ratings_1.map(lambda rows: convert_rating_data(rows))

train_ratings = ratings_col.join(train_users).map(lambda ((user, movie), (r1, r2)): (user, movie, r1))

train_ratings_list = train_ratings.collect()

for (user, movie, ratings) in train_ratings_list:
    user_dict_profile[user][movie] = ratings
    movie_dict_profile[movie][user] = ratings
# print user_dict_profile

similarity_list_1 = similar_data_rdd_1.collect()
similarity_list_2 = similar_data_rdd_2.collect()

for (movie1, movie2, similarity) in similarity_list_1:
    similarity_model_1[movie1][movie2] = similarity

for (movie2, movie1, similarity) in similarity_list_2:
    similarity_model_2[movie2][movie1] = similarity

sorted(similarity_model_1[movie1].items(), key=lambda x: x[1], reverse=False)
sorted(similarity_model_2[movie2].items(), key=lambda x: x[1], reverse=False)

for user, movie in test_users_list:
    rate = predictive_ratings(user, movie)

    if rate >= 0.0 and rate <= 5.0:
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

final_rdd.coalesce(1).saveAsTextFile("Prashanth_Manja_ItemBasedCF999.txt")
btw_0_1 = difference.filter(lambda (rating): rating >= 0 and rating < 1).count()
btw_1_2 = difference.filter(lambda (rating): rating >= 1 and rating < 2).count()
btw_2_3 = difference.filter(lambda (rating): rating >= 2 and rating < 3).count()
btw_3_4 = difference.filter(lambda (rating): rating >= 3 and rating < 4).count()
btw_4_5 = difference.filter(lambda (rating): rating >= 4).count()
MSE = difference.map(lambda line: line ** 2).mean()
RMSE = math.sqrt(MSE);

print(">=0 and <1:", btw_0_1)
print(">=1 and <2:", btw_1_2)
print(">=2 and <3:", btw_2_3)
print(">=3 and <4:", btw_3_4)
print(">=4 :", btw_4_5)
print("RMSE :", RMSE)
end_time = time.time()
total_time = end_time - start_time
print "Ouput written to the file"
print("Total_Execution_Time_Is-->", str(total_time))







