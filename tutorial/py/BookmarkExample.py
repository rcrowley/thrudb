#!/usr/bin/env python

import sys
sys.path.append("../gen-py")
sys.path.append("../../gen-py")

from time import time

from thrift import Thrift
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TFramedTransport, TMemoryBuffer
from thrift.protocol.TBinaryProtocol import TBinaryProtocol

from Thrudoc import Thrudoc, ttypes as ThrudocTypes
from Thrudex import Thrudex, ttypes as ThrudexTypes

from Bookmark.ttypes import Bookmark

# Config Constants
THRUDEX_PORT   = 11299;
THRUDOC_PORT   = 11291;

THRUDOC_BUCKET = "bookmarks";

THRUDEX_DOMAIN = "tutorial";
THRUDEX_INDEX  = "bookmarks";

def chunker(seed, size, func):   
    while True:
        chunk = func(THRUDOC_BUCKET, seed, size)
        yield chunk
        if len(chunk.responses) != size:
            break
        seed = chunk.seed

class BookmarkManager(object):
    def __init__(self):
        self.connect_to_thrudoc()
        self.connect_to_thrudex()

    def connect_to_thrudoc(self):
        socket = TSocket('localhost', THRUDOC_PORT)
        transport = TFramedTransport(socket)
        protocol = TBinaryProtocol(transport)
        self.thrudoc = Thrudoc.Client(protocol)

        transport.open()

    def connect_to_thrudex(self):
        socket = TSocket('localhost', THRUDEX_PORT)
        transport = TFramedTransport(socket)
        protocol = TBinaryProtocol(transport)
        self.thrudex = Thrudex.Client(protocol)

        transport.open()

    def serialize(self, b):
        mbuf = TMemoryBuffer()
        mbuf_protocol = TBinaryProtocol(mbuf)
        b.write(mbuf_protocol)
        return mbuf.getvalue()


    def deserialize(self, b_str):
        mbuf = TMemoryBuffer(b_str)
        mbuf_protocol = TBinaryProtocol(mbuf)
        b = Bookmark()
        b.read(mbuf_protocol)
        return b

    def load_tsv_file(self, fl):
        t0 = time()

        for line in open(fl, "rb"):
            bs = line.strip().split('\t')

            b = Bookmark()
            b.url   = bs[0]
            b.title = bs[1]
            b.tags  = bs[2]

            self.add_bookmark(b)

        self.thrudex.commitAll()

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
        bid = self.thrudoc.putValue(b_str)
        return bid

    def index_bookmark(self, b_id, b):
        doc = ThrudexTypes.DocMsg()
        doc.docid = b_id
        doc.domain = THRUDEX_DOMAIN
        doc.index = THRUDEX_INDEX
        doc.fields = []

        field = ThrudexTypes.Field()
        field.name = "title"
        field.value = b.title
        field.sortable = True
        field.stype = ThrudexTypes.StorageType.UNSTORED
        doc.fields.append(field)
  
        field = ThrudexTypes.Field()
        field.name = "tags"
        field.value = b.tags
        field.stype = ThrudexTypes.StorageType.UNSTORED
        doc.fields.append(field)

        self.thrudex.add(doc)

    def remove_all(self):
        t0   = time()
        seed = ""

        for ids in chunker(seed, 100, self.thrudoc.scan):
            docs = []
            for id in ids.elements:
                rm = ThrudexTypes.RemoveMsg()
                rm.domain = THRUDEX_DOMAIN
                rm.index = THRUDEX_INDEX
                rm.docid = id.key
                docs.append(rm)

            self.thrudex.removeList(docs)
            self.thrudoc.removeList(ids.elements)
            self.thrudex.commitAll()

        t1 = time()
        print "\n*Index cleared in: %.2f seconds*" % (t1-t0)

    def find(self, terms, random=False, sortby=None):
        print "\nSearching for:", terms
#        for k,v in options.iteritems():
#            print "\t", k, v

        t0 = time()

        q = ThrudexTypes.QueryMsg()
        q.domain = THRUDEX_DOMAIN
        q.index = THRUDEX_INDEX
        q.query = terms

        q.limit = 100
        #q.offset = 10

        if random:
            q.randomize = True
        if sortby:
            q.sortby = sortby

        ids = self.thrudex.query(q)
        if ids is None:
            return

        print "Found", ids.total, "bookmarks"

        if len(ids.ids) > 0:
            bm_strs = self.thrudoc.fetchList( self.create_doc_list(ids.ids))
            bms = [self.deserialize(bs) for bs in bm_strs]
            self.print_bookmarks(bms)

        t1 = time()
        
        print "Took: %.2f seconds" % (t1-t0)

    def create_doc_list(self, ids):
        docs = []
        for id in enumerate(ids):
            doc = ThrudocTypes.Element()
            doc.bucket = THRUDOC_BUCKET
            doc.key    = id
            docs.append(doc)

        return docs

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

