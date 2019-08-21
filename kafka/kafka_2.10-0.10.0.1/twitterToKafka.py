from kafka import SimpleProducer, KafkaClient
from tweepy.streaming import StreamListener
from tweepy import Stream
from tweepy import OAuthHandler


consumer_key =  /
consumer_secret = /
access_token = /
access_token_secret =  /


class KafkaListener(StreamListener):
    def on_data(self, streamData):
        producer.send_messages("twitterStream", streamData.encode('utf-8'))
        print (streamData)
        return True
    def on_error(self, status):
        print (status)

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
listener = KafkaListener()
stream = Stream(auth, listener)
stream.filter(track="twitterStream")
