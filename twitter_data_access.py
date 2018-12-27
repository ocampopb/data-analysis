import json
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener


consumer_key = 'opvOhY3r48FWaeOgBaTKaM1jn'
consumer_secret = 'iMa91FMeFbb4KbnwkS3VsVC5V8iEk3nwBBLet6Td3qF1qG80AI'
access_token = '293300953-qAzs31W5VFs5384kgTb0UimRMDM9zmYFPW5Y3gWU'
access_token_secret = 'XH2VVo038K9poyv0KSHZdhkqC12h50DfydHXAcJ1bKnAD'

auth = OAuthHandler(consumer_key,
                                        consumer_secret)
auth.set_access_token(access_token, access_token_secret)

class PrintListener(StreamListener):
    def on_status(self, status):
           if not status.text[:3] == 'RT ':
                 print(status.text)
                 print(status.author.screen_name, 
                    status.created_at, 
                    status.source, 
                    '\n')

    def on_error(self, status_code):
           print("Error code: {}".format(status_code))
           return True # Keep stream alive

    def on_timeout(self):
           print('Listener timed out!')
           return True # Keep stream alive


def print_to_terminal():
       listener = PrintListener()
       stream = Stream(auth, listener)
       languages = ('en',)
       stream.sample(languages=languages)


if  __name__ ==  '__main__':
    print_to_terminal()