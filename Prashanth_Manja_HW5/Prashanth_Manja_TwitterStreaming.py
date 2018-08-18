from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from collections import Counter, defaultdict
import sys
import random
from operator import itemgetter

consumer_key = "5CqvMzghdU8pYPwg2fxcOjBzs"
consumer_secret = "8GketYza4lu2EinfjvZvhI3BoW7pPwnAzvKuObVe1i3CXgstTI"
access_token = "1018040030575521793-4itk9IBP8q8UWIYqcfVfp54qIz02SU"
access_token_secret = "YXAKGeZ3iXrdx8ehp2x1dFMEz0Nucs0MIfCmiS3s8uagH"


class StdOutListener(StreamListener):
    mylist = []
    mydct = {}
    i = 0
    mylst = []
    len_list = []
    item_index = 0
    cnt = 0
    hash_dict = dict()
    tweet_len_dict = dict()
    hashtag_count_dict = defaultdict(lambda: 0)
    hashes_list = []

    sum_of_len = 0

    def decision(self, probability):
        return random.random() < probability

    def on_status(self, status):

        if self.item_index < 100:

            self.hashes_list = status.entities.get('hashtags')

            for each in self.hashes_list:
                if len(each['text']) != 0:
                    self.hashtag_count_dict[each['text']] += 1

            self.len_list.append(len(status.text))

            self.tweet_len_dict[self.item_index] = len(status.text)

            if len(self.len_list) == 100:
                self.sum_of_len = sum(self.len_list)

            if len(self.hashes_list) != 0:

                for each in self.hashes_list:
                    self.hash_dict[self.item_index] = each['text']

            self.item_index += 1

        else:

            self.item_index += 1
            self.prob = float(100.0/self.item_index)

            self.decision_to_include = self.decision(self.prob)

            if self.decision_to_include:
                index_to_replace_1 = random.uniform(1, 100)
                index = int(index_to_replace_1)

                if self.hash_dict.has_key(index):
                    hash_to_decrement = self.hash_dict[index]
                    self.hashtag_count_dict[hash_to_decrement] = self.hashtag_count_dict[hash_to_decrement] - 1

                    self.hash_dict[index] = None

                self.hashes_list = status.entities.get('hashtags')

                for each in self.hashes_list:
                    if len(each['text']) != 0:
                        self.hashtag_count_dict[each['text']] += 1
                        self.hash_dict[index] = each['text']


                self.sum_of_len = self.sum_of_len - self.tweet_len_dict[index] + len(status.text)

                self.average_twt_length = float(self.sum_of_len / 100.0)

                sorted_hashtags = sorted(self.hashtag_count_dict.items(), key=itemgetter(1),reverse=True)

                top_5_hashtags = sorted_hashtags[:5]

                print "*********************************************************************"
                print "The number of the twitter from beginning:" + str(self.item_index)
                print "Top 5 hot hashtags :"
                for each in top_5_hashtags:
                    print each[0] + ":" + str(each[1])
                print "The average length of the twitter is :" + str(self.average_twt_length)
                print "*********************************************************************"
                print "\n"

        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    stream.filter(track=['FIFA','USA','France','Trump','India','Los Angeles','NY','Maryland'], async=True,languages=['en'])

