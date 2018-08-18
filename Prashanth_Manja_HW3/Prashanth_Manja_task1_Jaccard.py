import itertools
from collections import defaultdict
import sys
from pyspark import SparkContext, SparkConf
from itertools import combinations
import time


start_time = time.time()
sc = SparkContext(appName='inf553')

number_of_bands = 40
row_size = 3
size_of_bin = 6
size_of_hash = number_of_bands * row_size

if(sys.argv.__len__() >1):
    
    input_file = sys.argv[1]
    
else:
    print("Please provide correct input_file_path")
    exit(0)


def readline(fname):
    rowline = str(fname).split(',')
    return (rowline[0], rowline[1])


def select_movie_user(rowData):
    return (int(rowData[1]), int(rowData[0]))


def createMatrix(rowData):
    a = 41
    b = 23
    returnDit = []

    for value in range(size_of_hash):
        hashMin = 111111111
        for i in rowData[1]:
            hashMin = min(hashMin, ((a + value) * i + (b + value)) % 671)
        returnDit.append(hashMin)
    return (rowData[0], returnDit)


file_info = sc.textFile(input_file)
data = file_info.map(lambda line: readline(line))

header_info = data.first()
data_mu = data.filter(lambda ratings: ratings != header_info)
mov_user_RDD = data_mu.map(lambda rowline: select_movie_user(rowline))
mov_user_rdd = mov_user_RDD.map(lambda name: (name[0], [name[1]])).reduceByKey(lambda a, b: a + b)
finalCompareRDD = mov_user_rdd.collectAsMap()
mov_user_rdd = mov_user_rdd.map(lambda x: (x[0], list(x[1])))


signatureMatrix = mov_user_rdd.map(createMatrix).collectAsMap()


start = 0
end = 3
eachBandCandiPairs = defaultdict()
eachBandCompareDict = defaultdict()


def callTogetCandiPairs(input):
    global eachBandCompareDict
    global eachBandCandiPairs
    combinations = itertools.combinations(input,2)
    for eachComb1 in combinations:
                if(eachComb1[0][1] == eachComb1[1][1]):
                    eachBandCandiPairs[tuple((eachComb1[0][0],eachComb1[1][0]))] = 1

buckets = 301
eachBucket = []

for band in range(number_of_bands):
        # print band
        for x,y in signatureMatrix.items():
            temp= signatureMatrix[x]
            eachSmallList= temp[start:end]
            # print eachSmallList
            tempSum =0
            for each in eachSmallList:
                tempSum += each*3
            sumOfTemp = tempSum % 101
            if(sumOfTemp in eachBandCompareDict.keys()):
                temp = eachBandCompareDict[sumOfTemp]
                temp.append(tuple((x,eachSmallList)))
                eachBandCompareDict[sumOfTemp] = temp
            else:
                temp = []
                temp.append(tuple((x,y)))
                eachBandCompareDict[sumOfTemp] = temp

        for each in eachBandCompareDict.keys():
            callTogetCandiPairs(eachBandCompareDict[each])
            eachBandCompareDict[each] = []
            # print "Ins" + str(band) + " " + str(len(eachBandCompareDict[each]))

        start = start + 3
        end = end+3

# print len(eachBandCandiPairs.keys())

# print finalCompareRDD

totalFullData =dict()

for each in eachBandCandiPairs.keys():
    lol1 = set(finalCompareRDD[each[0]])
    lol2 = set(finalCompareRDD[each[1]])
    tot = len(lol1 & lol2)/float(len(lol1|lol2))
    if(tot>= 0.5):
        totalFullData[tuple((each[0], each[1]))] = tot


total_data = totalFullData.items()
print len(totalFullData.items())

total_data.sort()
end_time = time.time()
total_time = end_time-start_time
#print("Total_Execution_Time_Is-->",str(total_time)) 


output_file = 'Prashanth_Manja_SimilarMovies_Jaccard.txt'
f = open(output_file,'w')

for k,v in total_data:
    f.write(str(k[0])+ ','+ str(k[1])+ ','+ str(v))
    f.write("\n")
print "Writing similar movies to file complete"

print("Total_Execution_Time_Is-->",str(total_time))

# Remaining is to print to the file