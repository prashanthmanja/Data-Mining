from pyspark import SparkContext
import itertools
from collections import defaultdict
import math
import time
import sys
from networkx import edge_betweenness_centrality 
import networkx as nx
from networkx.algorithms import community
from networkx.algorithms.community.centrality import girvan_newman
#from networkx.algorithms.community.centrality import greedy_modularity_communities
#import community


start_time = time.time()
sc = SparkContext()

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
    return (int(rowline[0]),1)

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
                connected_users.append(tuple((int(x),int(y))))

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


# import community
# dendo = community.generate_dendrogram(MG)
# print dendo


# from community import community_louvain

# partition1 = community_louvain.best_partition(G)

# print partition1

# par_list1 =  partition1.items()
# partition_rdd1 = sc.parallelize(par_list1)
# new_partition_rdd1 = partition_rdd1.map(lambda s: reversed(s))
# new_partition_rdd1.groupByKey().map(lambda x : (x[0], list(x[1]))).collect()


# compo = list(sorted(nx.connected_components(MG), key=len, reverse=True))

# for each in compo:
#     print list(each)


from networkx.algorithms.community.centrality import girvan_newman
communities_generator = girvan_newman(G)


top_level_communities = next(communities_generator)

next_level_communities = next(communities_generator)

final_list = sorted(map(sorted, next_level_communities))



# from networkx.algorithms.community.asyn_fluidc import asyn_fluidc
# comm_asyn = asyn_fluidc(MG,k=224)

# for item in comm_asyn:
#     print item

# from networkx.algorithms.community.label_propagation import asyn_lpa_communities
# comm_lpa = asyn_lpa_communities(MG)

# for item in comm_lpa:
#     print item

# from networkx.algorithms.community.label_propagation import label_propagation_communities
# comm_label = label_propagation_communities(MG)


# print list(comm_label)

# from networkx.algorithms.community import k_clique_communities
# c = list(k_clique_communities(MG, 2))
# print c

end_time = time.time()
total_time = end_time - start_time

output_file = 'Prashanth_Manja_Bonus_Community.txt'
f = open(output_file,'w')

for each1 in final_list:
    #print each1
    f.write("%s\n" % sorted(list(each1)))

print "Ouput written to the file"
print("Total_Execution_Time_Is-->", str(total_time))