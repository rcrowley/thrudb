#!/usr/bin/env ruby

$:.push('../gen-rb')

require 'thrift/transport/tsocket.rb'
require 'thrift/protocol/tbinaryprotocol.rb'

require 'Thrudoc'
require 'Thrudoc_types.rb'
require 'Thrudex'
require 'Thrudex_types.rb'

require 'Bookmark_types.rb'

#Config Constants
THRUDEX_PORT   = 11299;
THRUDOC_PORT   = 11291;

THRUDOC_BUCKET = "bookmarks";

THRUDEX_DOMAIN = "tutorial";
THRUDEX_INDEX  = "bookmarks";


class BookmarkManager

      def initialize
          connect_to_thrudoc()
          connect_to_thrudex()

          @mbuf           = TMemoryBuffer.new()
          @mbuf_protocol  = TBinaryProtocol.new(@mbuf)
      end

      def connect_to_thrudoc
          transport = TFramedTransport.new(TSocket.new('localhost', THRUDOC_PORT))
          protocol  = TBinaryProtocol.new(transport)
          @thrudoc  = Thrudoc::Client.new(protocol)

          transport.open()
      end

      def connect_to_thrudex
          transport = TFramedTransport.new(TSocket.new('localhost', THRUDEX_PORT))
          protocol  = TBinaryProtocol.new(transport)
          @thrudex = Thrudex::Client.new(protocol)

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


          @thrudex.commitAll()

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
         id    = @thrudoc.putValue( THRUDOC_BUCKET, b_str )

         return id
     end

     def index_bookmark(id,b)
         #
         #Indexing requires mapping the fields in our
         #bookmark object to a Thrudex Document
         #
         doc  = DocMsg.new()

         doc.docid  = id
         doc.domain = THRUDEX_DOMAIN
         doc.index  = THRUDEX_INDEX
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

         @thrudex.add( doc )
     end

     def remove_all
         t0 = Time.new

         #chunks of 100
         limit  = 100
         docs   = []
         seed   = nil;

         begin
                response = @thrudoc.scan(THRUDOC_BUCKET,seed,limit)

                response.elements.each { |r|
                   rm = RemoveMsg.new()
                   rm.domain = THRUDEX_DOMAIN
                   rm.index  = THRUDEX_INDEX
                   rm.docid  = r.key

                   docs << rm
                }


                @thrudex.removeList(docs)
                @thrudoc.removeList(response.elements)


                @thrudex.commitAll()


                seed = response.seed

        end while response.elements.length == limit


        t1 = Time.new

        print "\n*Index cleared in: "+(t1-t0).to_s+"*\n"
    end


    def find(terms, options = {})

        print "Searching for: "+terms
        options.each_pair{ |key, value| print " #{key.inspect}:#{value}" }
        print "\n"

        t0 = Time.new()

        q  = QueryMsg.new();

        q.domain = THRUDEX_DOMAIN
        q.index  = THRUDEX_INDEX
        q.query  = terms

        #q.limit = 10
        #q.offset= 10

        if options[:random] != nil
           q.randomize = true
        end

        if options[:sortby] != nil
           q.sortby = options[:sortby]
        end

        ids = @thrudex.query( q )

        if ids == nil
           return
        end

        print "Found "+ids.total.to_s+" bookmarks\n"

        if ids.ids.length > 0

           doc_list = @thrudoc.getList( create_doc_list(ids.ids) )
           bms      = []
           doc_list.each{ |doc|
                if doc.element.value != nil
                   bms << deserialize(doc.element.value)
                else
                   print "Error getting:"+doc.element.key+"\n"
                end
           }

           print_bookmarks(bms)
        end

        t1 = Time.new();
        print "Took: "+(t1-t0).to_s+"\n\n"
    end

    def create_doc_list(ids)
        docs = []

        ids.each{ |id|
              doc        = Element.new()
              doc.bucket = THRUDOC_BUCKET
              doc.key    = id

              docs << doc
        }

        return docs
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
