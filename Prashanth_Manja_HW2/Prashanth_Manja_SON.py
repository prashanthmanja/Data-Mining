
import time
import sys
import math
from operator import add
import itertools
from pyspark import SparkContext
from collections import defaultdict
from itertools import chain, combinations
import os

start_time = time.time()
sc = SparkContext("local[*]",appName='inf553')
#final_set_of_items = []
list_of_mov_user=[]


if(sys.argv.__len__() >1):
    casenumber = int(sys.argv[1])
    input_file_path = sys.argv[2]
    support = int(sys.argv[3])
else:
    print("Please provide correct input_file_path")
    exit(0)


## Data Cleaning - 
## Spliting the input rdd and selecting the 'userid' and 'movie_id'
def readline(fname):
    rowline = str(fname).split(',')   
    return (rowline[0], rowline[1])

### Rearranging the selected coloums for case 1
def select_user_movie(rowData):
    return (int(rowData[0]), int(rowData[1]))

### Rearranging the selected coloums for case 2
def select_movie_user(rowData):
    return (int(rowData[1]), int(rowData[0]))

def select_final_freq_set(fset):
    return (fset[0])

## Appending all the items from basket
def create_list(baskets):
    global list_of_mov_user
    for item in baskets:
        list_of_mov_user.append(item)
    return list_of_mov_user

### SON-PHASE_1- takes in rdd partitions and for each partition we compute singletons using 
### min-support for that node (here p_min_support)

## For singletons
def implement_phase1_SON_1(baskets):
    global list_of_mov_user,candidate_keys,p_min_support
    singleton = defaultdict(int)
    
    list_of_mov_user = []
    set_of_mov_user = set()

    mylist = create_list(baskets)
    for item in mylist:
        if itemset_size ==1 :
            for element in item:
                singleton[element]+=1
                set_of_mov_user.add(element)
    
    p_min_support = math.floor(support * len(list_of_mov_user)/itemcount)

    if itemset_size ==1 :
        set_of_mov_user = {k:v for k,v in singleton.items() if v>=p_min_support}
        set_of_mov_user = [citems for citems in chain(*[combinations(set_of_mov_user, 1)])] 
        return set_of_mov_user
    
## For items other than singletons
def implement_phase1_SON_2(baskets):

    global candidate_keys,itemset_size,list_of_mov_user,p_min_support
    mylist = create_list(baskets)
    p_min_support = math.floor(support * len(list_of_mov_user)/itemcount)
    candidate_keys = produce_candidate_keys(candidate_keys, itemset_size,p_min_support)
    set_of_mov_user = FrequentItems_stage1(candidate_keys, list_of_mov_user,p_min_support)
    keys_set_of_mov_users = set_of_mov_user.keys()

    return keys_set_of_mov_users

## SON-phase2
def implement_phase2_SON(baskets):

    global list_of_mov_user,candidate_keys
    
    phase2_list = create_list(baskets)

    output_list=[]
    output_list = FrequentItems_stage2(candidate_keys,phase2_list)
    return output_list

## Generate candidate Keys	
def produce_candidate_keys(ckeys, itemset_size,p_min_support):
    
    Items_split_up = set()
    result_ckeys = set()
    
    for key in ckeys:
        for item in key:
            Items_split_up.add(item)

    len_item_split_up = len(Items_split_up)
            
    if len_item_split_up >= itemset_size:
        combi_items = chain(*[combinations(Items_split_up, itemset_size)])
        result_ckeys = check_subset(combi_items, itemset_size,ckeys)
            
            
    return result_ckeys

## Checking if a combination is subset
def check_subset(cand_combinations,itemset_size,ckeys):
    cand_result=list()
    for each_comb in cand_combinations:
        Frequent_subsets_flag = True
        current_itemset_size = itemset_size - 1
        subset_cand_combinations = chain(*[combinations([values for values in each_comb], current_itemset_size)])
            
        for subset in subset_cand_combinations:
            key = tuple(sorted(subset))
            if key not in ckeys:
                Frequent_subsets_flag = False
                break
        if Frequent_subsets_flag:
            fkey = tuple(sorted(each_comb))
            cand_result.append(fkey)
            
            
    return cand_result

def FrequentItems_stage1(candiset, movie_list,p_Support):

    frequent_result = defaultdict(int)
    
    if candiset:
        movie_list_filtered = []

        for each_item in movie_list:
            max_len_candiset = len(max(candiset))
            each_item_len = len(each_item)
            if each_item_len >= max_len_candiset:
                movie_list_filtered.append(each_item)
        
        len_mov_list_filtered = len(movie_list_filtered)
        if len_mov_list_filtered < p_Support:
            return frequent_result

        frequent_items = defaultdict(int)
        for mov_user in candiset:
            for items in movie_list:
                mov_user_set = set(mov_user)
                if mov_user_set.issubset(set(items)):
                    tup_mov_user = tuple(sorted(mov_user))
                    if frequent_items[tup_mov_user] == p_Support:
                        frequent_result[tup_mov_user] = p_Support
                        break
                    elif True :
                        frequent_items[tuple(sorted(mov_user))] += 1

    return frequent_result

def FrequentItems_stage2(candiset, movie_list):
    frequent_items = defaultdict(int)
    
    if candiset:
        movie_list_filtered = []
        len_candi_set = len(max(candiset))
        for each_item in movie_list:
            len_each_item = len(each_item)
            if len_each_item>=len_candi_set:
                movie_list_filtered.append(each_item)
        for movie_user in candiset:
            for movie_list in movie_list_filtered:
                    movie_set = set(movie_user)
                    if movie_set.issubset(set(movie_list)):
                        frequent_items[tuple(sorted(movie_user))] += 1
    
    key_val_set = frequent_items.items()
    result_list_stage2 =[]
    for k,v in key_val_set :

        key_value_tuple = tuple((k,v))
        result_list_stage2.append(key_value_tuple)
    return result_list_stage2

## Entry-Point- Create RDD,Clean RDD,
def compute_frequent_items(cnum,fname,sthreshold):
    
    global support,itemset_size,candidate_keys,itemcount
    
    support = sthreshold
    itemset_size = 1
    candidate_keys =[]
    to_write_freq_itemsets= defaultdict(list)
    
     
    file_info = sc.textFile(fname)
    data = file_info.map(lambda line : readline(line))
    
    #num_of_partitions = file_info.getNumPartitions()
    #p_min_support = math.floor(support/num_of_partitions)
    ## Extraacting header row
    header_info = data.first()

    ### Selects all the rows except the header row
    data_mu = data.filter(lambda ratings: ratings != header_info)
    
    ## For CASE 1 & CASE 2
    if cnum == 1:
        user_mov_RDD = data_mu.map(lambda rowline :select_user_movie(rowline)) 
        rdd_buckets = user_mov_RDD.map(lambda name: (name[0], [ name[1] ])).reduceByKey(lambda a, b: a + b).map(lambda x : (list(x[1]))).cache()
        #print rdd_buckets
    elif cnum == 2:
        mov_user_RDD = data_mu.map(lambda rowline :select_movie_user(rowline))
        rdd_buckets = mov_user_RDD.map(lambda name: (name[0], [ name[1] ])).reduceByKey(lambda a, b: a + b).map(lambda x : (list(x[1]))).cache()
    
    itemcount = rdd_buckets.count()
    final_set_of_items = []
    itemset_size = 1
    candidate_keys =[]
   
    while itemset_size==1 or candidate_keys:
        
        if itemset_size ==1:
            candidate_keys = rdd_buckets.mapPartitions(implement_phase1_SON_1).distinct().collect()        
            

            phase2_out_final_candidate_keys= rdd_buckets.mapPartitions(implement_phase2_SON)
        
            result_keys = phase2_out_final_candidate_keys.reduceByKey(add).filter(lambda a: a[1]>=sthreshold)
        
            f_result_keys = result_keys.map(lambda ele: select_final_freq_set(ele))
        
            output_result_keys = f_result_keys.collect()

        else:
            candidate_keys = rdd_buckets.mapPartitions(implement_phase1_SON_2).distinct().collect()        

            phase2_out_final_candidate_keys= rdd_buckets.mapPartitions(implement_phase2_SON)
        
            result_keys = phase2_out_final_candidate_keys.reduceByKey(add).filter(lambda a: a[1]>=sthreshold)
        
            f_result_keys = result_keys.map(lambda ele: select_final_freq_set(ele))
        
            output_result_keys = f_result_keys.collect()


        for elements in output_result_keys:
            final_set_of_items.append(elements)
            
        itemset_size+=1

    ## Sorting the output
    final_set_of_items = sorted(final_set_of_items)
    final_set_of_items.sort(key= lambda x :len(x),reverse=False)
    
    ## writing into file
    output_file = 'Prashanth_Manja_SON_Output_.case'+str(cnum)+str('-')+str(sthreshold)+'.txt'
    f = open(output_file,'w')
    
 
    for elements in final_set_of_items:
        to_write_freq_itemsets[len(elements)].append(elements)
     

    key_value_fitems = to_write_freq_itemsets.items()   
    for k,v in key_value_fitems:
        if k==1:
            f.write(", ".join(["("+str(x[0])+")" for x in v]))
            f.write("\n")
            f.write("\n")
        elif k!=1:
            f.write(", ".join([str(x) for x in v]))
            f.write("\n")
            f.write("\n")
    
    print(len(final_set_of_items))
    end_time = time.time()
    total_time = end_time-start_time
    print("Total_Execution_Time_Is-->",str(total_time))


#casenumber = 1
#input_file_path = "r1.csv"
#support = 120
compute_frequent_items(casenumber,input_file_path,support)

