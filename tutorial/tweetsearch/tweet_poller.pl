#!/usr/bin/perl

use strict;
use warnings;

use lib '../gen-perl';

#Thrift components
use Thrift;
use Thrift::MemoryBuffer;
use Thrift::BinaryProtocol;
use Thrift::Socket;
use Thrift::FramedTransport;

#Thrudb
use Thrudex;
use Thrudoc;

use JSON::Any;
use LWP::UserAgent;

package TweetCatcher;

use Date::Parse qw(str2time);
use Data::Dumper;

#Config
use constant THRUDEX_PORT    => 11299;
use constant THRUDOC_PORT    => 11291;

use constant THRUDOC_BUCKET  => "tweets";
use constant THRUDEX_INDEX   => "tweets";


sub new
{
    my $classname = shift;
    my $self      = {};

    $self->{j}    = new JSON::Any();

    #Create object
    bless($self,$classname);

    #Connect to thrudb
    $self->connect_to_thrudoc();
    $self->connect_to_thrudex();

    return $self;
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

        $self->{thrudoc}->admin("create_bucket", THRUDOC_BUCKET());
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

        $self->{thrudex}->admin("create_index", THRUDEX_INDEX());

    }; if($@){
        die $@->{message} if UNIVERSAL::isa($@,"Thrift::TException");
        die $@;
    }
}

sub run
{
    my $self = shift;
    my $j    = $self->{j};
    my $since_id = "";
    my $count   = 0;
    my $ua = LWP::UserAgent->new;
    $ua->timeout(10);

    while(1){
        my $url = "http://twitter.com/statuses/public_timeline.json?since_id=$since_id"."&r=".rand();

        my $response = $ua->get($url);

        #just skip on error
        if (!$response->is_success) {
            next;
        }

        my $timeline = $j->decode($response->content) || [];
        my $list_id;



        foreach my $tweet ( @$timeline ){
            eval{
                $self->index_doc( $tweet );
                $self->save_doc( $tweet );

                $count++;
                $since_id = $tweet->{id};
            }; if($@){
                #warn(Dumper($tweet));
            }
        }

        print STDERR "Loaded $count\r";
    }
}


sub index_doc
{
    my $self    = shift;
    my $tweet   = shift;
    my $thrudex = $self->{thrudex};

    #
    #Indexing requires mapping the fields in our
    #bookmark object to a Thrudex Document
    #
    my $doc      = new Thrudex::Document();

    $doc->key($tweet->{id});
    $doc->index( THRUDEX_INDEX() );

    my $field = new Thrudex::Field();

    #
    #text
    #
    $field->key("text");
    $field->value($tweet->{text});
    push(@{$doc->{fields}}, $field);


    $field = new Thrudex::Field();

    #
    #date
    #
    $field->key("date");
    $field->value(str2time($tweet->{created_at}));
    $field->sortable(1);
    push(@{$doc->{fields}}, $field);


    $thrudex->put( $doc );
}

sub save_doc
{
    my $self    = shift;
    my $tweet   = shift;
    my $thrudoc = $self->{thrudoc};
    my $j       = $self->{j};

    $thrudoc->put( THRUDOC_BUCKET(), $tweet->{id}, $j->encode($tweet) );
}

1;

#####Run####
my $tc = new TweetCatcher();
$tc->run();
