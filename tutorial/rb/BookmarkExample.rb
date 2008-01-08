#!/usr/bin/env ruby

$:.push('../gen-rb')

require 'thrift/transport/tsocket.rb'
require 'thrift/protocol/tbinaryprotocol.rb'

require 'Thrudoc'
require 'Thrudoc_types.rb'
require 'Thrucene'
require 'Thrucene_types.rb'

require 'Bookmark_types.rb'

#Config Constants
THRUCENE_PORT   = 11299;
THRUDOC_PORT    = 11291;

THRUCENE_DOMAIN = "tutorial";
THRUCENE_INDEX  = "bookmarks";


class BookmarkManager

      def initialize
          connect_to_thrudoc()
          connect_to_thrucene()

          @mbuf           = TMemoryBuffer.new()
          @mbuf_protocol  = TBinaryProtocol.new(@mbuf)
      end

      def connect_to_thrudoc
          transport = TFramedTransport.new(TSocket.new('localhost', THRUDOC_PORT))
          protocol  = TBinaryProtocol.new(transport)
          @thrudoc  = Thrudoc::Client.new(protocol)

          transport.open()
      end

      def connect_to_thrucene
          transport = TFramedTransport.new(TSocket.new('localhost', THRUCENE_PORT))
          protocol  = TBinaryProtocol.new(transport)
          @thrucene = Thrucene::Client.new(protocol)

          transport.open()
      end

      def serialize(b)
          @mbuf.resetBuffer()
          b.write(@mbuf_protocol)

          return @mbuf.getBuffer()
      end

      def deserialize(b_str)
          b = Bookmark.new()

          #set the buffer
          @mbuf.resetBuffer(b_str)

          b.read(@mbuf_protocol)

          return b
      end

      def load_tsv_file(file)

          t0 = Time.new;

          open(file).each { |x|
                x.chomp
                bs = x.split("\t")

                b = Bookmark.new
                b.url   = bs[0]
                b.title = bs[1]
                b.tags  = bs[2]

                add_bookmark(b)
          }


          @thrucene.commitAll()

          t1 = Time.new()

          print "*Indexed file in: "+(t1-t0).to_s+"*\n\n"
      end

      def add_bookmark(b)

        id = store_bookmark(b)
        index_bookmark(id,b)
      end

      def get_bookmark(id)

        b_str   = @thrudoc.fetch(id)

        if b_str.length == 0
           return
        end

        return deserialize(b_str)
     end


     def store_bookmark(b)
         b_str = serialize(b)
         id    = @thrudoc.add( b_str )

         return id
     end

     def index_bookmark(id,b)
         #
         #Indexing requires mapping the fields in our
         #bookmark object to a Thrucene Document
         #
         doc  = DocMsg.new()

         doc.docid  = id
         doc.domain = THRUCENE_DOMAIN
         doc.index  = THRUCENE_INDEX
         doc.fields = []

         field = Field.new()


         #title
         field.name    = "title"
         field.value   = b.title
         field.sortable= true
         doc.fields << field

         #tags
         field = Field.new()
         field.name    = "tags"
         field.value   = b.tags
         doc.fields << field

         @thrucene.add( doc )
     end

     def remove_all
         t0 = Time.new

         #chunks of 100
         offset = 0
         limit  = 1000
         docs   = []

         begin
                ids = @thrudoc.listIds(offset,limit)

                ids.each { |id|
                   rm = RemoveMsg.new()
                   rm.domain = THRUCENE_DOMAIN
                   rm.index  = THRUCENE_INDEX
                   rm.docid  = id

                   docs << rm
                }


                @thrucene.removeList(docs)
                @thrudoc.removeList(ids)

                offset += limit
                @thrucene.commitAll()
        end while ids.length == limit


        t1 = Time.new

        print "\n*Index cleared in: "+(t1-t0).to_s+"*\n"
    end


    def find(terms, options = {})

        print "Searching for: "+terms
        options.each_pair{ |key, value| print " #{key.inspect}:#{value}" }
        print "\n"

        t0 = Time.new()

        q  = QueryMsg.new();

        q.domain = THRUCENE_DOMAIN
        q.index  = THRUCENE_INDEX
        q.query  = terms

        #q.limit = 10
        #q.offset= 10

        if options[:random] != nil
           q.randomize = true
        end

        if options[:sortby] != nil
           q.sortby = options[:sortby]
        end

        ids = @thrucene.query( q )

        if ids == nil
           return
        end

        print "Found "+ids.total.to_s+" bookmarks\n"

        if ids.ids.length > 0
           bm_strs = @thrudoc.fetchList(ids.ids)
           bms     = []
           bm_strs.each{ |bm_str| bms << deserialize(bm_str) }

           print_bookmarks(bms)
        end

        t1 = Time.new();
        print "Took: "+(t1-t0).to_s+"\n\n"
    end


    def print_bookmarks(bookmarks)
        i = 1

        bookmarks.each { |b|
              print i.to_s+" \ttitle :\t"+b.title+"\n"
              print "\turl   :\t("+b.url+")\n"
              print "\ttags  :\t("+b.tags+")\n"
              i += 1
        }
    end

end


##################
begin

        bm = BookmarkManager.new();

        bm.load_tsv_file("../bookmarks.tsv")

        bm.find("tags:(+css +examples)",{ :random => 1})

        bm.find("title:(linux)",{:sortby => "title"})

        bm.remove_all()

rescue TException => tx
        print 'TException: ', tx.message, "\n"
end
