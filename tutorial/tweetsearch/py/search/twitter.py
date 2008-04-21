#!/usr/bin/env python


from thrift import Thrift
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TFramedTransport, TMemoryBuffer
from thrift.protocol.TBinaryProtocol import TBinaryProtocol

from Thrudoc import Thrudoc, ttypes as ThrudocTypes
from Thrudex import Thrudex, ttypes as ThrudexTypes

from random import random
from time import time

import socket
# timeout in seconds
TIMEOUT = 10
socket.setdefaulttimeout(TIMEOUT)
import urllib2
import cjson
import sys

# Config Constants
THRUDEX_PORT   = 11299;
THRUDOC_PORT   = 11291;

THRUDOC_BUCKET = "tweets";
THRUDEX_INDEX  = "tweets";

class TweetCatcher(object):
    def __init__(self, since_id=None):
        self.connect_to_thrudoc()
        self.connect_to_thrudex()
        self.since_id = since_id
        self.count = 0

    def connect_to_thrudoc(self):
        socket = TSocket('localhost', THRUDOC_PORT)
        transport = TFramedTransport(socket)
        protocol = TBinaryProtocol(transport)
        self.thrudoc = Thrudoc.Client(protocol)
        transport.open()
        self.thrudoc.admin("create_bucket", THRUDOC_BUCKET)

    def connect_to_thrudex(self):
        socket = TSocket('localhost', THRUDEX_PORT)
        transport = TFramedTransport(socket)
        protocol = TBinaryProtocol(transport)
        self.thrudex = Thrudex.Client(protocol)
        transport.open()
        self.thrudex.admin("create_index", THRUDEX_INDEX)
    
    def index_tweet(self, tweet):
        doc = ThrudexTypes.Document()
        doc.key = str(tweet["id"])
        doc.index = THRUDEX_INDEX
        doc.fields = []

        field       = ThrudexTypes.Field()
        field.key   = "text"
        field.value = tweet["text"].encode('utf-8')
        doc.fields.append(field)
            
        field       = ThrudexTypes.Field()
        field.key   = "date"
        field.value = tweet["created_at"]
        field.sortable = True
        doc.fields.append(field)
        self.thrudex.put(doc)

    def save_tweet(self, tweet):
        self.thrudoc.put(THRUDOC_BUCKET, str(tweet["id"]), cjson.encode(tweet))    

    def run(self):
        while True:
            # the random paramater used to avoid http caching by upstream provider
            url = "http://twitter.com/statuses/public_timeline.json?since_id=%s&r=%s" % (self.since_id, random())
            #just skip on error
            try:
                response = urllib2.urlopen(url)
                json = response.read()
                tweets = cjson.decode(json)
            except Exception, e:
                print e                
                continue               
            for tweet in tweets:
                # somehow sometimes tweet is not a dictionary, check to make sure
                if isinstance(tweet, dict):
                    # if sth serious wrong happened in thrudb, better to log it and continue
                    try:
                        self.index_tweet(tweet)
                        self.save_tweet(tweet)
                        self.count = self.count + 1
                        self.since_id = tweet["id"]
                    except Exception, e:
                        print e                                                                
                        continue 
            print "loaded %s tweets, last since_id %s" % (self.count, self.since_id)

class TweetManager(object):
    def __init__(self):
        self.connect_to_thrudoc()
        self.connect_to_thrudex()

    def connect_to_thrudoc(self):
        socket = TSocket('localhost', THRUDOC_PORT)
        transport = TFramedTransport(socket)
        protocol = TBinaryProtocol(transport)
        self.thrudoc = Thrudoc.Client(protocol)
        transport.open()
        self.thrudoc.admin("create_bucket", THRUDOC_BUCKET)

    def connect_to_thrudex(self):
        socket = TSocket('localhost', THRUDEX_PORT)
        transport = TFramedTransport(socket)
        protocol = TBinaryProtocol(transport)
        self.thrudex = Thrudex.Client(protocol)
        transport.open()
        self.thrudex.admin("create_index", THRUDEX_INDEX)
    
    def search_tweet(self, terms, offset=0, limit=10):
        q = ThrudexTypes.SearchQuery()
        q.index = THRUDEX_INDEX
        q.query = '+text:(%s)' % terms
        q.offset = offset
        q.limit = limit
        q.sortby = 'date'
        q.desc = True     
 
        ids = self.thrudex.search(q)
        tweets = []             
        if len(ids.elements) > 0:
            list_response = self.thrudoc.getList(self.create_doc_list(ids.elements))
            for ele in list_response:
                 if ele.element.value != '':
                   tweet = cjson.decode(ele.element.value)                   
                   tweet["profile_image_url"] = tweet["user"]["profile_image_url"].replace("\\", "")
                   tweet["user_name"] = tweet["user"]["screen_name"]
                   tweet["text"] = tweet["text"].replace("\\", "")
                   tweets.append(tweet)                        
        return ids.total, tweets

    def create_doc_list(self, ids):
        docs = []
        for pointer, ele in enumerate(ids):
            doc = ThrudocTypes.Element()
            doc.bucket = THRUDOC_BUCKET
            doc.key    = ele.key
            docs.append(doc)

        return docs
   
        
if __name__ == "__main__":
    import daemonize as dm
    dm.daemonize('/dev/null','/tmp/twitter.log','/tmp/twitter.log')
    tc = TweetCatcher()
    tc.run()
