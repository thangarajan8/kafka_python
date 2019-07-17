# -*- coding: utf-8 -*-
"""
Created on Sat Jul  6 15:28:02 2019

@author: Thangarajan
"""
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')


from datetime import datetime
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener

#consumer key, consumer secret, access token, access secret.
consumer_key='XXXXXXXXXXXXXXXXXXXXXXXXXXX'
consumer_secret='XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
access_token='XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
access_token_secret='XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'

class listener(StreamListener):

    def on_data(self, data):
        print('Publishing data to Kafka {}'.format(datetime.now()))
        producer.send('tweet', str.encode(data))
        return(True)

    def on_error(self, status):
        print(status)

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

twitterStream = Stream(auth, listener())
twitterStream.filter(track=["ICCRules"])