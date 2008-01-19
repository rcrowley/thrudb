#!/usr/bin/env perl

use strict;
use warnings;

#Thrift components
use Thrift;
use Thrift::MemoryBuffer;
use Thrift::BinaryProtocol;
use Thrift::Socket;
use Thrift::FramedTransport;

#Bookmark
use lib "../gen-perl";
use Tutorial::Types;

#Thrudb
use Thrudex;
use Thrudoc;


package BookmarkManager;
use Time::HiRes qw(gettimeofday);
use Data::Dumper;

#Config
use constant THRUDEX_PORT   => 11299;
use constant THRUDOC_PORT    => 11291;

use constant THRUDOC_BUCKET  => "bookmarks";

use constant THRUDEX_DOMAIN => "tutorial";
use constant THRUDEX_INDEX  => "bookmarks";


sub new
{

    my $classname = shift;
    my $self      = {};

    #Used to serialize bookmark obj to string
    my $mbuf           = new Thrift::MemoryBuffer();
    my $mbuf_protocol  = new Thrift::BinaryProtocol($mbuf);

    $self->{mbuf}          = $mbuf;
    $self->{mbuf_protocol} = $mbuf_protocol;

    #Create object
    bless($self,$classname);

    #Connect to thrudb
    $self->connect_to_thrudoc();
    $self->connect_to_thrudex();

    return $self;
}

sub serialize_bookmark
{
    my $self = shift;
    my $b    = shift;

    my $mbuf          = $self->{mbuf};
    my $mbuf_protocol = $self->{mbuf_protocol};

    $mbuf->resetBuffer();

    $b->write($mbuf_protocol);

    return $mbuf->getBuffer();
}

sub deserialize_bookmark
{
    my $self  = shift;
    my $b_str = shift;
    my $b     = new Tutorial::Bookmark();

    my $mbuf          = $self->{mbuf};
    my $mbuf_protocol = $self->{mbuf_protocol};

    #set the buffer
    $mbuf->resetBuffer($b_str);

    $b->read($mbuf_protocol);

    return $b;
}

sub connect_to_thrudoc
{
    my $self = shift;

    eval{
       my $socket    = new Thrift::Socket("localhost",THRUDOC_PORT());
       my $transport = new Thrift::FramedTransport($socket);
       my $protocol  = new Thrift::BinaryProtocol($transport);
       $self->{thrudoc}  = new ThrudocClient($protocol);

       $transport->open();
    }; if($@) {
        die $@->{message} if UNIVERSAL::isa($@,"Thrift::TException");
        die $@;
    }
}

sub connect_to_thrudex
{
    my $self = shift;

    eval{
        my $socket    = new Thrift::Socket("localhost",THRUDEX_PORT());
        my $transport = new Thrift::FramedTransport($socket);
        my $protocol  = new Thrift::BinaryProtocol($transport);
        $self->{thrudex}  = new ThrudexClient($protocol);

        $transport->open();
    }; if($@){
        die $@->{message} if UNIVERSAL::isa($@,"Thrift::TException");
        die $@;
    }
}

sub load_tsv_file
{
    my $self = shift;
    my $file = shift;


    my $t0 = gettimeofday();

    open(BMFILE, "<$file") || die $!;

    while( chomp( my $line = <BMFILE> ) ){
        my($url,$title,$tags) = split("\t",$line);

        my $b = new Tutorial::Bookmark();

        $b->url  ( $url   );
        $b->title( $title );
        $b->tags ( $tags  );

        $self->add_bookmark($b);
    }

    close(BMFILE);

    $self->{thrudex}->commitAll();

    my $t1 = gettimeofday();

    print "*Indexed file in: ".($t1-$t0)."*\n\n";
}

sub add_bookmark
{
    my $self    = shift;
    my $b       = shift;

    die("Bad bookmark") unless defined $b;

    my $id = $self->store_bookmark($b);
    $self->index_bookmark($id,$b);
}

sub get_bookmark
{
    my $self    = shift;
    my $id      = shift;

    my $thrudoc = $self->{thrudoc};
    my $b_str   = $thrudoc->get(THRUDOC_BUCKET(),$id);

    return unless defined $b_str;

    return $self->deserialize_bookmark($b_str);
}

sub store_bookmark
{
    my $self     = shift;
    my $b        = shift;
    my $thrudoc  = $self->{thrudoc};

    my $b_str    = $self->serialize_bookmark($b);

    my $id = $thrudoc->putValue(THRUDOC_BUCKET(), $b_str );

    return $id;
}


sub index_bookmark
{
    my $self     = shift;
    my $thrudex = $self->{thrudex};

    my $id       = shift;
    my $b        = shift;

    #
    #Indexing requires mapping the fields in our
    #bookmark object to a Thrudex Document
    #
    my $doc      = new Thrudex::DocMsg();

    $doc->docid($id);
    $doc->domain( THRUDEX_DOMAIN() );
    $doc->index ( THRUDEX_INDEX()  );

    my $field = new Thrudex::Field();

    #
    #title
    #
    $field->name("title");
    $field->value($b->title);
    $field->sortable(1);
    push(@{$doc->{fields}}, $field);

    #
    #tags
    #
    $field = new Thrudex::Field();
    $field->name("tags");
    $field->value($b->tags);
    push(@{$doc->{fields}}, $field);

    $thrudex->add( $doc );
}

sub remove_all
{
    my $self     = shift;

    my $thrudoc  = $self->{thrudoc};
    my $thrudex  = $self->{thrudex};


    my $t0 = gettimeofday();

    #chunks of 100
    my $limit  = 100;
    my $r;
    my $seed;
    do{

        $r = $thrudoc->scan(THRUDOC_BUCKET(), $seed , $limit);

        if(@{$r->elements} > 0 ){

            my @thrudex_docs;

            foreach my $el (@{ $r->elements }){
                #
                my $rm = new Thrudex::RemoveMsg();
                $rm->domain( THRUDEX_DOMAIN() );
                $rm->index ( THRUDEX_INDEX()  );
                $rm->docid($el->key);

                push(@thrudex_docs, $rm);
            }

            $thrudex->removeList(\@thrudex_docs);
            $thrudoc->removeList($r->elements);

            $thrudex->commitAll();

        }

        $seed = $r->{seed};

    }while(@{$r->elements} == $limit);


    my $t1 = gettimeofday();

    print "\n*Index cleared in: ".($t1-$t0)."*\n";

}

sub find
{
    my $self  = shift;
    my $terms = shift;

    my $options = shift || {};

    return unless defined $terms;

    print "Searching for: '$terms' ".join(",",map{$_.":".$options->{$_}} keys %$options)."\n";
    my $t0 = gettimeofday();

    my $thrudex = $self->{thrudex};
    my $thrudoc  = $self->{thrudoc};
    my $query    = new Thrudex::QueryMsg();

    $query->domain( THRUDEX_DOMAIN() );
    $query->index(  THRUDEX_INDEX() );
    $query->query($terms);

    #$query->limit(10);
    #$query->offset(10);

    $query->randomize(1)               if exists $options->{random};
    $query->sortby($options->{sortby}) if exists $options->{sortby};

    my $ids = $thrudex->query( $query );

    return unless defined $ids;

    print "Found ".$ids->total." bookmarks\n";

    if(@{$ids->{ids}} > 0){

        my $response = $thrudoc->getList( $self->create_doc_list($ids->{ids}) );

        my @bookmarks;

        foreach my $r (@$response){

            if( $r->element->value ne "" ){

                my $bm = $self->deserialize_bookmark( $r->element->value );

                push(@bookmarks, $bm);
            }else{

                warn($r->ex->what);
            }

        }

        $self->print_bookmarks(\@bookmarks);
    }

    my $t1 = gettimeofday();
    print "Took: ".($t1-$t0)."\n\n";
}

sub create_doc_list
{
    my $self     = shift;
    my $ids      = shift;

    my @docs;

    foreach my $id (@$ids){
        my $el = new Thrudoc::Element();

        $el->bucket( THRUDOC_BUCKET() );
        $el->key( $id );

        push(@docs,$el);
    }

    return \@docs;
}

sub print_bookmarks
{
    my $self      = shift;
    my $bookmarks = shift;

    my $i = 1;
    foreach my $b (@$bookmarks){
        print "$i .\ttitle :\t".$b->title."\n";
        print "\turl   :\t(".$b->url.")\n";
        print "\ttags  :\t(".$b->tags.")\n";
        $i++;
    }
}

1;

#################################
#            Main
#################################
my $bm = new BookmarkManager();

my ($t0,$t1);
eval{

    $bm->load_tsv_file("../bookmarks.tsv");

    $bm->find("tags:(+css +examples)",{ random => 1});

    $bm->find("title:(linux)",{sortby => "title"});

    $bm->remove_all();

};if($@){
    die $@->{message} if UNIVERSAL::isa($@,"Thrift::TException");
    die $@;
}
