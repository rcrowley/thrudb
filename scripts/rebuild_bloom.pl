#!/usr/bin/perl

use strict;
use warnings;

use Net::Amazon::S3;
use Data::Dumper;

#
#Parse config (dead simple)
#
sub parse_config
{
    my $config_file = shift;
    my $c           = {};

    die("Missing config file") unless defined $config_file && -e $config_file;

    open(CFILE, "<$config_file") || die $1;

    while(<CFILE>){
        my($k,$v) = ( $_ =~ m/^\s*([^=^\s]+)\s*=\s*([^\s]+)/ );

        next unless defined $k && defined $v;

        next if $k =~ /\#/;
        next if $v =~ /\#/;

        $c->{$k} = $v;
    }

    close(CFILE);

    return $c;
}



my $c = parse_config(shift);


die "Missing Doc root" unless defined $c->{DOC_ROOT} && -d $c->{DOC_ROOT};

#
#Validate params for Amazon backup
#
if( $c->{BACKEND_TYPE} eq "S3" || $c->{BACKEND_TYPE} eq "DISK+S3" ){

    die "Missing or Invalid AWS_ACCESS_KEY"
        unless defined $c->{AWS_ACCESS_KEY};

    die "Missing or Invalid AWS_SECRET_ACCESS_KEY"
        unless defined $c->{AWS_SECRET_ACCESS_KEY};

    die "Missing S3_BUCKET_NAME"
        unless defined $c->{S3_BUCKET_NAME};

    recover_s3();

} elsif( $c->{BACKEND_TYPE} eq "DISK" ){

    recover_disk();

} else {

    die "Unknown backend";
}


sub recover_s3
{
    rename($c->{DOC_ROOT}."/bloom.idx", $c->{DOC_ROOT}."/bloom.idx.".time())
        if -e $c->{DOC_ROOT}."/bloom.idx";

    open(BLOOM, ">$c->{DOC_ROOT}/bloom.idx") || die $!;


    my $s3     = Net::Amazon::S3->new({ aws_access_key_id     => $c->{AWS_ACCESS_KEY},
                                        aws_secret_access_key => $c->{AWS_SECRET_ACCESS_KEY} });

    my $bucket = $s3->bucket($c->{S3_BUCKET_NAME});

    my $marker = undef;

    while(my $res    = $bucket->list({ marker => $marker, delimiter=>"|" }) ){

        foreach my $key ( @{ $res->{keys} } ) {

            #next unless $key =~ m/^[a-f0-9\-]+$/i;

            print BLOOM $key->{key}."\n";
        }

        $marker = $res->{next_marker};

        last unless $res->{is_truncated};
    }

    close(BLOOM);
}


sub recover_disk
{
    rename($c->{DOC_ROOT}."/bloom.idx", $c->{DOC_ROOT}."/bloom.idx.".time())
        if -e $c->{DOC_ROOT}."/bloom.idx";


    open(DISKF, "find $c->{DOC_ROOT} -type f |") || die $!;

    open(BLOOM, ">$c->{DOC_ROOT}/bloom.idx") || die $!;

    while( chomp(my $line = <DISKF>) ){

        my ($f) = ($line =~ /.+\/([a-f0-9\-]+)$/ig);

        next unless defined $f;

        print BLOOM $f."\n";
    }

    close(BLOOM);

    close(DISKF);
}

warn("Done");
