#!/usr/bin/perl

use strict;
use warnings;

use lib "../../gen-perl";

use CGI qw/:standard/;
use HTML::Template;

$ENV{HTML_TEMPLATE_ROOT} = ".";
$|++;

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

package TweetScan;
use Time::HiRes qw(gettimeofday);
use HTML::Entities;
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
       $socket->setRecvTimeout(5000);

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
	$socket->setRecvTimeout(5000);

        my $transport = new Thrift::FramedTransport($socket);
        my $protocol  = new Thrift::BinaryProtocol($transport);
        $self->{thrudex}  = new ThrudexClient($protocol);

        $transport->open();


    }; if($@){
        die $@->{message} if UNIVERSAL::isa($@,"Thrift::TException");
        die $@;
    }
}

sub search
{
    my $self    = shift;

    my $terms   = shift;
    my $offset  = shift || 0;

    my $thrudoc = $self->{thrudoc};
    my $thrudex = $self->{thrudex};

    my $j       = $self->{j};
    my $limit   = 10;

    my $query   = new Thrudex::SearchQuery();

    $query->index(  THRUDEX_INDEX() );
    $query->query("+text:($terms)");
    $query->sortby("date");
    $query->desc(1);

    $query->limit($limit);
    $query->offset($offset);


    my $ids = $thrudex->search( $query );
    my @tweets;

    #
    #
    #
    if(@{$ids->{elements}} > 0){
        my $response = $thrudoc->getList( $self->create_doc_list($ids->{elements}) );

        foreach my $r (@$response){

            if( $r->element->value ne "" ){

                my $tweet = $j->decode($r->element->value);
                $tweet->{profile_image_url} = $tweet->{user}{profile_image_url};
                $tweet->{user_name} = $tweet->{user}{screen_name};
                push(@tweets, $tweet);
            }else{
                warn($r->{ex}->{what});
            }
        }
    }

    return ($ids->total,\@tweets);
}

sub create_doc_list
{
    my $self     = shift;
    my $ids      = shift;

    my @docs;

    foreach my $id (@$ids){
        my $el = new Thrudoc::Element();

        $el->bucket( THRUDOC_BUCKET() );
        $el->key( $id->key );

        push(@docs,$el);
    }

    return \@docs;
}

sub handle
{
    my $self  = shift;
    my $q     = new CGI;
    
    my $template = new HTML::Template(filename          => "template.html",
				      die_on_bad_params => 0,
				      global_vars       => 1,
				      cache             => 1,
				      loop_context_vars => 1
	);
    
    
    my $terms = $q->param("terms");
    my $offset= $q->param("offset") || 0;
    eval{
	
	if( defined $terms && $terms ne ""){
	    
	    $terms = HTML::Entities::encode($terms);
	    
	    #escape this bad boy
	    my $pterms   = $terms;
	    $pterms      =~ s/([\+\-\&\|\!\(\)\{\}\[\]\^\"\~\*\?\:\\])/\\$1/g;
	    $pterms      = "+".join(" +",split(/\s+/,$pterms));

	    
	    my $t0      = gettimeofday();
	    
	    my ($total,$tweets) = $self->search( $pterms, $offset );
	    
	    my $t1      = gettimeofday();
	    
	    $template->param( 
		terms      => $terms, 
		offset     => $offset, 
		total      => $total, 
		tweets     => $tweets,
		current    => $offset,
		next       => ( $total > ($offset + @$tweets) ? ($offset + @$tweets) : undef),
		prev       => ( $offset > 0  ? ($offset - 10)." " : undef),
		
		rendertime => sprintf("%0.2f", ($t1 - $t0) ));
	}

    }; if($@){
	warn(Dumper($@));
    }

    print "content-type: text/html\n\n";
    print $template->output();
    
}

1;

#####Run####
my $t = new TweetScan();
$t->handle();
