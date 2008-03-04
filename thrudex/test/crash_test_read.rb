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

total=0
t0=Time.new

q=Thrudex::SearchQuery.new
q.index="test"
q.query="title:thequerydoesntmatter"

while true
  total+=1

  res=thrudex.search(q)
  #puts "got #{res.elements.size}"

  if total % 100==0 then
    puts "#{total} queries/sec=#{ total/(Time.new-t0)}"
  end
end
