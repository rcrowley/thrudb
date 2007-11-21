#!/usr/bin/perl

use strict;
use warnings;


#Thrift components
use Thrift;
use Thrift::MemoryBuffer;
use Thrift::BinaryProtocol;
use Thrift::Socket;
use Thrift::FramedTransport;

#Thrudb
use Thrucene;
use Thrudoc;

#Bookmark
use lib "../gen-perl";
use Tutorial::Types;


package BookmarkManager;
use Time::HiRes qw(gettimeofday);

#Config
use constant THRUCENE_PORT   => 11299;
use constant THRUDOC_PORT    => 11291;

use constant THRUCENE_DOMAIN => "tutorial";
use constant THRUCENE_INDEX  => "bookmarks";


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
    $self->connect_to_thrucene();

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

sub connect_to_thrucene
{
    my $self = shift;

    eval{
        my $socket    = new Thrift::Socket("localhost",THRUCENE_PORT());
        my $transport = new Thrift::FramedTransport($socket);
        my $protocol  = new Thrift::BinaryProtocol($transport);
        $self->{thrucene}  = new ThruceneClient($protocol);

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

    $self->{thrucene}->commitAll();

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
    my $b_str   = $thrudoc->fetch($id);

    return unless defined $b_str;

    return $self->deserialize_bookmark($b_str);
}

sub store_bookmark
{
    my $self     = shift;
    my $b        = shift;
    my $thrudoc  = $self->{thrudoc};

    my $b_str    = $self->serialize_bookmark($b);

    my $id = $thrudoc->store( $b_str );

    return $id;
}


sub index_bookmark
{
    my $self     = shift;
    my $thrucene = $self->{thrucene};

    my $id       = shift;
    my $b        = shift;

    #
    #Indexing requires mapping the fields in our
    #bookmark object to a Thrucene Document
    #
    my $doc      = new Thrucene::DocMsg();

    $doc->docid($id);
    $doc->domain( THRUCENE_DOMAIN() );
    $doc->index ( THRUCENE_INDEX()  );

    my $field = new Thrucene::Field();

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
    $field = new Thrucene::Field();
    $field->name("tags");
    $field->value($b->tags);
    push(@{$doc->{fields}}, $field);

    $thrucene->add( $doc );
}

sub remove_all
{
    my $self     = shift;

    my $thrudoc  = $self->{thrudoc};
    my $thrucene = $self->{thrucene};


    my $t0 = gettimeofday();

    #chunks of 100
    my $offset = 0;
    my $limit  = 100;
    my $ids;

    do{

        $ids = $thrudoc->fetchIds($offset,$limit);

        my @docs;
        foreach my $id (@$ids){
            my $rm = new Thrucene::RemoveMsg();
            $rm->domain( THRUCENE_DOMAIN() );
            $rm->index ( THRUCENE_INDEX()  );
            $rm->docid($id);

            push(@docs, $rm);
        }

        $thrucene->removeList(\@docs);
        $thrudoc->removeList($ids);

        $offset += $limit;

        $thrucene->commitAll();

    }while(@$ids == $limit);


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

    my $thrucene = $self->{thrucene};
    my $thrudoc  = $self->{thrudoc};
    my $query    = new Thrucene::QueryMsg();

    $query->domain( THRUCENE_DOMAIN() );
    $query->index(  THRUCENE_INDEX() );
    $query->query($terms);

    #$query->limit(10);
    #$query->offset(10);

    $query->randomize(1)               if exists $options->{random};
    $query->sortby($options->{sortby}) if exists $options->{sortby};

    my $ids = $thrucene->query( $query );

    return unless defined $ids;

    print "Found ".$ids->total." bookmarks\n";
    if( @{$ids->{ids}} > 0 ){

        my $bm_strs = $thrudoc->fetchList($ids->{ids});

        my @bookmarks = map{ $self->deserialize_bookmark($_) } @$bm_strs;

        $self->print_bookmarks(\@bookmarks);
    }

    my $t1 = gettimeofday();
    print "Took: ".($t1-$t0)."\n\n";
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
