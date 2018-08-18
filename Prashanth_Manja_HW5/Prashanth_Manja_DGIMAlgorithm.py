from pyspark.streaming import StreamingContext
from pyspark import SparkContext
import math
from math import pow

sc = SparkContext("local[*]", "NetworkWordCount")
ssc = StreamingContext(sc, 10)
sc.setLogLevel(logLevel="OFF")
streams_line = ssc.socketTextStream("localhost", 9999)

size_of_window = 1000
time_period = 1000
original_num_of_1s = 0
estimated_num_of_1s = 0
original_num_of_bits = 0
size_of_buckets = []
time_stamp = 0
buckets_dict = {}
i_items = 0
number_bits = 0
window = 0
new_list = []
max_num_of_bucket_size = int(math.log(1000) / math.log(2)) + 1

for p in range(max_num_of_bucket_size):
    baskets = math.pow(2, p)
    buckets_dict[baskets] = []

size_of_buckets = sorted(buckets_dict.keys())


def check_and_update_buckets(buckets_dict, a, time_stamp):
    for each_basket in size_of_buckets:
        num_of_similar_size_buckets = len(buckets_dict[each_basket])
        if num_of_similar_size_buckets > 2:
            del buckets_dict[each_basket][0]
            to_be_added_to_next_bucket = buckets_dict[each_basket].pop(0)
            last_bucket = size_of_buckets[-1]
            temp_buff = size_of_buckets
            if each_basket != last_bucket:
                next_basket = 2 * each_basket
                buckets_dict[next_basket].append(to_be_added_to_next_bucket)

        elif num_of_similar_size_buckets <= 2:

            break


def compute_count(stream):
    global time_stamp, original_num_of_bits, i_items, original_num_of_1s, estimated_num_of_1s, number_bits, window, new_list, total_1s, length_of_new_list, total_ones
    collection_of_bits = stream.collect()
    window = 0
    for each_bit in collection_of_bits:
        window = 1 % size_of_window
        key_buckets = buckets_dict.keys()
        time_stamp = (time_stamp + 1) % size_of_window
        for each_bucket_size in key_buckets:
            bucket_values = buckets_dict[each_bucket_size]
            for buck_time_stamp in bucket_values:
                if buck_time_stamp == time_stamp:
                    buckets_dict[each_bucket_size].remove(buck_time_stamp)

        entry_bit = int(each_bit)

        original_num_of_bits = (original_num_of_bits + 1) % size_of_window

        len_of_new_list = len(new_list)
        if len_of_new_list != size_of_window:
            new_list.append(entry_bit)

        elif len_of_new_list == size_of_window:
            oldest_bit = new_list.pop(0)
            new_list.append(entry_bit)
            if oldest_bit == 1:
                original_num_of_1s = original_num_of_1s - oldest_bit
            else:
                pass

        if entry_bit == 1:
            number_bits += 1
            original_num_of_1s = original_num_of_1s + 1
            buckets_dict[1].append(time_stamp)
            check_and_update_buckets(buckets_dict, entry_bit, time_stamp)

        # i_items = (i_items + 1) % time_period
        #
        # if original_num_of_bits == 0:
        #     total_1s = original_num_of_1s
        #     bit_one = total_1s
        #     original_num_of_1s = 0

    length_of_new_list = len(new_list)
    if length_of_new_list >= size_of_window:
        window = 1
        estimated_num_of_1s = 0
        for each_key in size_of_buckets:
            window += 1
            value = buckets_dict[each_key]
            for temp_stamp in value:
                temp = temp_stamp
                big_bucket = each_key
                estimated_num_of_1s = estimated_num_of_1s + each_key

        last_bucket_half = (big_bucket / 2)
        half_of_last_bucket = last_bucket_half
        estimated_num_of_1s = estimated_num_of_1s - half_of_last_bucket
        estimated_num_of_1s = int(estimated_num_of_1s)
        total_ones = original_num_of_1s
        print ("************************************************************************")
        print("\n")
        print "Estimated number of ones in the last 1000 bits: ", estimated_num_of_1s
        print "Actual number of ones in the last 1000 bits: ", total_ones
        print("\n")
        print ("************************************************************************")


streams_line.foreachRDD(lambda stream: compute_count(stream))
ssc.start()
ssc.awaitTermination()


