
from pyspark import SparkContext
import time
from collections import defaultdict
import sys
import networkx as nx

start_time = time.time()
sc = SparkContext('local', 'HW4')

if(sys.argv.__len__() >1):
    ratings_file = sys.argv[1]
else:
    print("Please provide correct input_file_path")
    exit(0)

def readusermoviedata(fname):
    rowline = str(fname).split(',')
    return (int(rowline[0]), int(rowline[1]))


def readuserdata(fname):
    rowline = str(fname).split(',')
    return (int(rowline[0]), 1)


ratings_data_rdd = sc.textFile(ratings_file)
header_info_train = ratings_data_rdd.first()
ratings_data = ratings_data_rdd.filter(lambda element: element != header_info_train)

users_movies = ratings_data.map(lambda line: readusermoviedata(line)).groupByKey()

filtered_users_movies = users_movies.map(lambda data: (data[0], set(data[1]))).filter(lambda row: len(row[1]) >= 9)

filtered_users_1 = ratings_data.map(lambda line: readuserdata(line))
filtered_users = filtered_users_1.distinct().map(lambda user: user[0]).collect()

filtered_users_movies_list = filtered_users_movies.collect()

filtered_users_movies_dict = dict(filtered_users_movies_list)
connected_users_dict = defaultdict(lambda :0)
connected_users = []
for x in filtered_users:
    for y in filtered_users:
        if x!=y:
            user_movie_inter = filtered_users_movies_dict[x].intersection(filtered_users_movies_dict[y])
            if len(user_movie_inter)>=9:
                connected_users_dict[(x, y)] = 1


connected_users_rdd = sc.parallelize(connected_users_dict.keys())

neighbours_1  = connected_users_rdd.groupByKey().map(lambda (user,level1):(user,set(level1))).collect()

neighbour = dict(neighbours_1)


G = nx.Graph()
G.add_edges_from(connected_users_dict.keys())
degree = G.degree
#print degree[1]
edges = G.edges()

edict = list(edges)

edict_new =  {x:0 for x in edict}

for i in filtered_users:
    leaves = list(set(filtered_users) - set(neighbour[i]))
    i_list = []
    i_list.append(i)
    leaves = list(set(leaves) - set(i_list))
    level1 = set()

    for x in neighbour[i]:
        level1.add((int(i), x))

    for u in level1:
        u = tuple(sorted(u))
        edict_new[u] += 1

    for p in leaves:
        parents = set()
        for x in neighbour[i]:

            if p in neighbour[x]:
                parents.add((i, x))
                parents.add((x, p))

        if parents != None:
            for e in parents:
                g = tuple(sorted(e))
                len_of_parents = len(parents)
                edict_new[g] += (1 / (float(len_of_parents)))

edict_new.update((x, (y-1)) for x, y in edict_new.items())

edict_new_list = edict_new.items()

ed_list = [(x[0][0], x[0][1], x[1]) for x in edict_new_list]

MG = nx.Graph()
MG.add_weighted_edges_from(ed_list)
m = MG.number_of_edges()
n1_components = 1
denom = 2 * m

sorted_edict = [key for (key, val) in sorted(edict_new.items(), key=lambda x: x[1], reverse=True)]
count = 0

def compute_modular(community_lists):
    mod_result1 =0
    global denom
    #print "len of comm list : " + str(len(community_lists))
    for each in community_lists:
        each = list(each)
        leng = len(each)
        for ii in range(0, leng):
            i = each[ii]
            ki = len(neighbour[i])
            for jj in range(ii, leng):
                    j = each[jj]
                    kj = len(neighbour[j])
                    Aij = connected_users_dict.get((i, j), 0)
                    part2 = float(ki*kj)/denom
                    mod_result1 += (Aij - part2)
    mod_result1 = mod_result1/denom
    #print mod_result1, len(community_lists)
    return mod_result1


final_list = list(nx.connected_components(MG))
prev_mod = compute_modular(list(nx.connected_components(MG)))

while len(sorted_edict) != 0:
    max_value = sorted_edict[0]
    del sorted_edict[0]
    MG.remove_edge(*max_value)
    n2_components = nx.number_connected_components(MG)

    if n1_components != n2_components:
        n1_components = n2_components
        community = nx.connected_components(MG)
        comm_list = list(community)
        curr_mod = compute_modular(comm_list)
        if curr_mod > prev_mod:
            prev_mod = curr_mod
            final_list = comm_list
            count = n1_components

print(count)

end_time = time.time()
total_time = end_time - start_time

output_file = 'Prashanth_Manja_Community.txt'
f = open(output_file,'w')

for each1 in final_list:
    #print each1
    f.write("%s\n" % sorted(list(each1)))

print "Ouput written to the file"
print("Total_Execution_Time_Is-->", str(total_time))