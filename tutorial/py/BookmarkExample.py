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
THRUDEX_INDEX  = "bookmarks";

def chunker(seed, size, func):   
    while True:
        chunk = func(THRUDOC_BUCKET, seed, size)
        yield chunk
        if len(chunk.elements) != size:
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

        self.thrudex.admin("create_index", THRUDEX_INDEX)

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
        bid = self.thrudoc.putValue(THRUDOC_BUCKET, b_str)
        return bid

    def index_bookmark(self, b_id, b):
        doc = ThrudexTypes.Document()
        doc.key = b_id
        doc.index = THRUDEX_INDEX
        doc.fields = []

        field       = ThrudexTypes.Field()
        field.key   = "title"
        field.value = b.title
        field.sortable = True
        doc.fields.append(field)
            
        field       = ThrudexTypes.Field()
        field.key   = "tags"
        field.value = b.tags
        doc.fields.append(field)
        self.thrudex.put(doc)

    def remove_all(self):
        t0   = time()
        seed = ""

        for ids in chunker(seed, 100, self.thrudoc.scan):
            docs = []
            for id in ids.elements:
                rm = ThrudexTypes.Element()            
                rm.index = THRUDEX_INDEX
                rm.key = id.key
                docs.append(rm)

            self.thrudex.removeList(docs)
            self.thrudoc.removeList(ids.elements)


        t1 = time()
        print "\n*Index cleared in: %.2f seconds*" % (t1-t0)

    def find(self, terms, random=False, sortby=None):
        print "\nSearching for:", terms

        t0 = time()

        q = ThrudexTypes.SearchQuery()
        q.index = THRUDEX_INDEX
        q.query = terms

        if random:
            q.randomize = True
        if sortby:
            q.sortby = sortby

        ids = self.thrudex.search(q)
        if ids is None:
            return

        print "Found %d bookmarks" % ids.total

        if len(ids.elements) > 0:
            list_response = self.thrudoc.getList( self.create_doc_list(ids.elements))
            bms = []
            for ele in list_response:
                 if ele.element.value != '':
                   bms.append(self.deserialize(ele.element.value))
                 else:
                   print 'value empty for key: %s' % ele.element.key
            self.print_bookmarks(bms)

        t1 = time()
        
        print "Took: %.2f seconds" % (t1-t0)

    def create_doc_list(self, ids):
        docs = []
        for pointer, ele in enumerate(ids):
            doc = ThrudocTypes.Element()
            doc.bucket = THRUDOC_BUCKET
            doc.key    = ele.key
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

