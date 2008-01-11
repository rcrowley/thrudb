#!/usr/bin/env python

import sys
sys.path.append("../gen-py")
sys.path.append("../../gen-py")

from time import time

from thrift import Thrift
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TFramedTransport, TMemoryBuffer
from thrift.protocol.TBinaryProtocol import TBinaryProtocol

from Thrudoc import Thrudoc
from Thrucene import Thrucene, ttypes as ThruceneTypes

from Bookmark.ttypes import Bookmark

# Config Constants
THRUCENE_PORT   = 11299;
THRUDOC_PORT    = 11291;

THRUCENE_DOMAIN = "tutorial";
THRUCENE_INDEX  = "bookmarks";

def chunker(start, size, func):
    offset = start
    while True:
        chunk = func(offset, size)
        yield chunk
        if len(chunk) != size:
            break
        offset += size

class BookmarkManager(object):
    def __init__(self):
        self.connect_to_thrudoc()
        self.connect_to_thrucene()

#        self.mbuf = TMemoryBuffer()
#        self.mbuf_protocol = TBinaryProtocol(self.mbuf)
    
    def connect_to_thrudoc(self):
        socket = TSocket('localhost', THRUDOC_PORT)
        transport = TFramedTransport(socket)
        protocol = TBinaryProtocol(transport)
        self.thrudoc = Thrudoc.Client(protocol)

        transport.open()

    def connect_to_thrucene(self):
        socket = TSocket('localhost', THRUCENE_PORT)
        transport = TFramedTransport(socket)
        protocol = TBinaryProtocol(transport)
        self.thrucene = Thrucene.Client(protocol)

        transport.open()

    def serialize(self, b):
        mbuf = TMemoryBuffer()
        mbuf_protocol = TBinaryProtocol(mbuf)
        b.write(mbuf_protocol)
        return mbuf.getvalue()
#        self.mbuf.resetBuffer()
#        b.write(self.mbuf_protocol)
#        return self.mbuf.getBuffer()

    def deserialize(self, b_str):
        mbuf = TMemoryBuffer(b_str)
        mbuf_protocol = TBinaryProtocol(mbuf)
        b = Bookmark()
        b.read(mbuf_protocol)
        return b
#        b = Bookmark()
#        mbuf.resetBuffer(b_str)
#        b.read(self.mbuf_protocol)
#        return b

    def load_tsv_file(self, fl):
        t0 = time()

        for line in open(fl, "rb"):
            bs = line.strip().split('\t')

            b = Bookmark()
            b.url   = bs[0]
            b.title = bs[1]
            b.tags  = bs[2]

            self.add_bookmark(b)

        self.thrucene.commitAll()

        t1 = time()
        
        print "\n*Indexed file in: %.2f seconds*" % (t1-t0)

    def add_bookmark(self, b):
        bid = self.store_bookmark(b)
        self.index_bookmark(bid, b)

    def get_bookmark(self, bid):
        b_str = self.thrudoc.fetch(bid)
        if len(b_str) == 0:
            return
        return self.deserialize(b_str)

    def store_bookmark(self, b):
        b_str = self.serialize(b)
        bid = self.thrudoc.add(b_str)
        return bid

    def index_bookmark(self, b_id, b):
        doc = ThruceneTypes.DocMsg()
        doc.docid = b_id
        doc.domain = THRUCENE_DOMAIN
        doc.index = THRUCENE_INDEX
        doc.fields = []

        field = ThruceneTypes.Field()
        field.name = "title"
        field.value = b.title
        field.sortable = True
        field.stype = ThruceneTypes.StorageType.UNSTORED
        doc.fields.append(field)
  
        field = ThruceneTypes.Field()
        field.name = "tags"
        field.value = b.tags
        field.stype = ThruceneTypes.StorageType.UNSTORED
        doc.fields.append(field)

        self.thrucene.add(doc)

    def remove_all(self):
        t0 = time()

        for ids in chunker(0, 1000, self.thrudoc.listIds):
            docs = []
            for id in ids:
                rm = ThruceneTypes.RemoveMsg()
                rm.domain = THRUCENE_DOMAIN
                rm.index = THRUCENE_INDEX
                rm.docid = id
                docs.append(rm)

            self.thrucene.removeList(docs)
            self.thrudoc.removeList(ids)
            self.thrucene.commitAll()

        t1 = time()
        print "\n*Index cleared in: %.2f seconds*" % (t1-t0)

    def find(self, terms, random=False, sortby=None):
        print "\nSearching for:", terms
#        for k,v in options.iteritems():
#            print "\t", k, v

        t0 = time()

        q = ThruceneTypes.QueryMsg()
        q.domain = THRUCENE_DOMAIN
        q.index = THRUCENE_INDEX
        q.query = terms

        q.limit = 100
        #q.offset = 10

        if random:
            q.randomize = True
        if sortby:
            q.sortby = sortby

        ids = self.thrucene.query(q)
        if ids is None:
            return

        print "Found", ids.total, "bookmarks"

        if len(ids.ids) > 0:
            bm_strs = self.thrudoc.fetchList(ids.ids)
            bms = [self.deserialize(bs) for bs in bm_strs]
            self.print_bookmarks(bms)

        t1 = time()
        
        print "Took: %.2f seconds" % (t1-t0)
    
    def print_bookmarks(self, bookmarks):
        for i, b in enumerate(bookmarks):
            print "%d\ttitle:\t%s" % (i+1, b.title)
            print "\turl:\t(%s)" % b.url
            print "\ttags:\t(%s)" % b.tags



if __name__ == "__main__":
    bm = BookmarkManager()
    bm.load_tsv_file("../bookmarks.tsv")
    bm.find("tags:(+css +examples)", random=True)
    bm.find("title:(linux)", sortby="title")
    bm.remove_all()

