
from pyspark import SparkContext
import math
import time
import sys
import networkx as nx



if(sys.argv.__len__() >1):
    ratings_file = sys.argv[1]
else:
    print("Please provide correct input_file_path")
    exit(0)

start_time = time.time()
sc = SparkContext()

def readusermoviedata(fname):
    rowline = str(fname).split(',')
    return (int(rowline[0]), int(rowline[1]))



def readuserdata(fname):
    rowline = str(fname).split(',')
    return (int(rowline[0]),1)


ratings_data_rdd = sc.textFile(ratings_file)
header_info_train = ratings_data_rdd.first()
ratings_data = ratings_data_rdd.filter(lambda element: element != header_info_train)

users_movies =ratings_data.map(lambda line: readusermoviedata(line)).groupByKey().sortByKey()

filtered_users_movies = users_movies.map(lambda data: (data[0],set(data[1]))).filter(lambda row: len(row[1])>=9)

filtered_users_1 = ratings_data.map(lambda line: readuserdata(line))
filtered_users = filtered_users_1.distinct().sortByKey().map(lambda user: user[0]).collect()

filtered_users_movies_list = filtered_users_movies.collect()


filtered_users_movies_dict = dict(filtered_users_movies_list)

connected_users = []
for x in filtered_users:
    for y in range(1,len(filtered_users)+1):
        #print y
        if x!=y:
            #print x
            #print type(filtered_users_movies_dict[x])
            user_movie_inter = filtered_users_movies_dict[x].intersection(filtered_users_movies_dict[y])
            if len(user_movie_inter)>=9:
                #connected_users += tuple((long(x),long(y)))
                connected_users.append(tuple((int(x),int(y))))

connected_users_rdd = sc.parallelize(connected_users)

neighbours_1  = connected_users_rdd.groupByKey().sortByKey().map(lambda (user,level1):(user,set(level1))).collect()

neighbour = dict(neighbours_1)

G = nx.Graph()

G.add_edges_from(connected_users)

edges = G.edges()

edict = list(edges)

edict_new =  { x:0 for x in edict}


for i in filtered_users:
    leaves = list(set(filtered_users) - set(neighbour[i]))
    #print len(leaves)
    i_list = []
    i_list.append(i)
    leaves = list(set(leaves) - set(i_list))
    #print len(leaves)
    level1 = set()
    
    for x in neighbour[i]:
        level1.add((int(i),x))
    
    #print level1
    for u in level1:
        u = tuple(sorted(u))
        #print u
        #print edict_new[u]
        edict_new[u]+=1
        #print edict_new[u]
    
    #print edict_new.values()
    for p in leaves:
        parents = set()
        for x in neighbour[i]:
            #print type(p)
            if p in neighbour[x]:
                parents.add((i,x))
                parents.add((x,p))
            #print len(parents)
                
        #print parents
        if parents!=None:
            for e in parents:
                #print e
                #print type(e)
                g = tuple(sorted(e))
                len_of_parents = len(parents)
                edict_new[g] += (1/(float(len_of_parents)))
                

edict_new.update((x,(y-1)) for x, y in edict_new.items())

edict_new_list = edict_new.items()

edict_new_list.sort()

end_time = time.time()
total_time = end_time-start_time

output_file = 'Prashanth_Manja_Betweeness.txt'
f = open(output_file,'w')

for k,v in edict_new_list:
    f.write('('+str(k[0])+ ','+ str(k[1])+ ','+ str(v)+')')
    f.write("\n")

print "Ouput written to the file"
print("Total_Execution_Time_Is-->",str(total_time))

