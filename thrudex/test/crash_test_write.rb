#!/usr/bin/env ruby

genrb=File.dirname(__FILE__)+"/gen-rb"
require 'thrift/transport/tsocket.rb'
require 'thrift/protocol/tbinaryprotocol.rb'
$:.push(genrb)
require 'Thrudex'
$:.pop

transport = TFramedTransport.new(TSocket.new("localhost", 9099))
protocol  = TBinaryProtocol.new(transport)
thrudex  = Thrudex::Thrudex::Client.new(protocol)
transport.open()

puts "connected to thrudex"

thrudex.admin("create_index", "test")

doc=Thrudex::Document.new
field=Thrudex::Field.new
doc.fields=[field]
total=0
t0=Time.now
while true
  total+=1
  doc.index="test"
  doc.key="#{rand}"

  field.key="title"
  field.value="#{rand(10000)}"

  thrudex.put(doc)

  if total % 1000==0 then
    puts "#{total} inserts/sec=#{ total/(Time.new-t0)}"
  end
end
