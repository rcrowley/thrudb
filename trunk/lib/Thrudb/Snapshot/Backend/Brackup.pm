#
# Copyright (c) 2007- T Jake Luciani
# Distributed under the New BSD Software License
#
# See accompanying file LICENSE or visit the Thrudb site at:
# http://thrudb.googlecode.com
#
#
package Thrudb::Snapshot::Backend::Brackup;

use strict;
use warnings;
use Data::Dumper;

require 5.6.0;

use base 'Thrudb::Snapshot';
use File::Temp;


sub new {
    my $classname = shift;

    my $self = $classname->SUPER::new(@_);

    #
    #All brackup meta files should go here
    #
    chdir("/var/tmp");

    $self->validate_config();
    $self->write_brackup_config();


    return $self;
}


sub validate_config
{
    my $self = shift;
    my $c    = $self->{c};

    chomp (my $brackup          = `which brackup`);
    chomp (my $brackup_target   = `which brackup-target`);
    chomp (my $brackup_restore  = `which brackup-restore`);

    die "Can't locate brackup"         if $brackup         eq "";
    die "Can't locate brackup-target"  if $brackup_target  eq "";
    die "Can't locate brackup-restore" if $brackup_restore eq "";

    die "Missing or Invalid DOC_ROOT"
        unless defined $c->{DOC_ROOT} && -d $c->{DOC_ROOT};

    die "Missing or Invalid BRACKUP_TYPE"
        unless defined $c->{BRACKUP_TYPE} && $c->{BRACKUP_TYPE} =~ m/^(Filesystem|Amazon)$/;


    #
    #Validate params for Amazon backup
    #
    if( $c->{BRACKUP_TYPE} eq "Amazon" ){

        die "Missing or Invalid AWS_ACCESS_KEY"
            unless defined $c->{AWS_ACCESS_KEY};

        die "Missing or Invalid AWS_SECRET_ACCESS_KEY"
            unless defined $c->{AWS_SECRET_ACCESS_KEY};

    }


    #
    #Validate params for Filesystem backup
    #
    if( $c->{BRACKUP_TYPE} eq "Filesystem" ){

        die "Missing or Invalid BRACKUP_TARGET_DIR"
            unless defined $c->{BRACKUP_TARGET_DIR} && -d $c->{BRACKUP_TARGET_DIR};

    }


    #
    #Make sure doc root has no trailing /
    #
    $c->{DOC_ROOT}           =~ s/\/\s*$//;
    $self->{doc_root}        = $c->{DOC_ROOT};

    #
    #Save brackup executable locations
    #
    $self->{brackup}         = $brackup;
    $self->{brackup_target}  = $brackup_target;
    $self->{brackup_restore} = $brackup_restore;
}

sub write_brackup_config
{
    my $self = shift;
    my $c    = $self->{c};

    my $tmp  = new File::Temp( UNLINK => 1, SUFFIX => '.tmp' );


    print $tmp "[SOURCE:thrudb]\n";
    print $tmp "path = $c->{DOC_ROOT}/\n";
    print $tmp "ignore = ^.snapshots/\n";
    print $tmp "ignore = ^.brackup-digest.db\n";
    print $tmp "noatime=1\n";
    print $tmp "chunk_size = 5m\n\n";

    print $tmp "[TARGET:backups]\n";
    print $tmp "type = $c->{BRACKUP_TYPE}\n";
    print $tmp "keep_backups = 2\n";

    if( $c->{BRACKUP_TYPE} eq "Amazon" ) {
        print $tmp "aws_access_key_id     = $c->{AWS_ACCESS_KEY}\n";
        print $tmp "aws_secret_access_key = $c->{AWS_SECRET_ACCESS_KEY}\n";
    }

    if( $c->{BRACKUP_TYPE} eq "Filesystem" ){
        print $tmp "path = $c->{BRACKUP_TARGET_DIR}\n";
    }

    close($tmp);

    $self->{config} = $tmp;
}

sub get_latest
{
    my $self      = shift;

    my $cmd       = "$self->{brackup_target} backups list_backups --config=$self->{config}";
    my $output    = `$cmd 2>&1`;

    #
    #Parse the output
    #
    my @snapshots = ( $output =~ m/(thrudb\S+)/mg );

    if(@snapshots == 0){
        return undef;
    }

    @snapshots = sort{ $a cmp $b } @snapshots;

    return $snapshots[-1];
}

sub create
{
    my $self = shift;


    my $cmd    = "$self->{brackup} --to=backups --from=thrudb --config=$self->{config}";

    my $output = `$cmd 2>&1`;

    die($output) if $output ne "";

    my $latest = $self->get_latest().".brackup";


    system("mkdir -p $self->{doc_root}/.snapshots");
    system("touch $self->{doc_root}/.snapshots/$latest");

    print $latest."\n";
}

sub recover
{
    my $self   = shift;
    my $safe   = shift;

    my ($cmd,$output);
    my $latest = $self->get_latest();

    if( -e $self->{doc_root}."/.snapshots/".$latest.".brackup" ){
        die("Already at latest version\n");
    }

    #
    #Get latest snapshot (if not already there)
    #
    unless( -e $latest ){

        $cmd    = "$self->{brackup_target} backups get_backup $latest --config=$self->{config}";
        $output = `$cmd 2>&1`;

        die($output) if $output ne "";
    }

    #
    #Move old doc root out of the way
    #
    if( $safe ){
        system("rm -fr $self->{doc_root}"."-backup");
        system("mv -f $self->{doc_root} $self->{doc_root}-backup" );
        mkdir($self->{doc_root});
    } else {
        system("rm -fr $self->{doc_root}");
        mkdir($self->{doc_root});
    }

    $latest .= ".brackup";

    #
    #Finally, restore
    #
    $cmd    = "$self->{brackup_restore} --from=$latest --to=$self->{doc_root} --all --config=$self->{config}";
    $output = `$cmd 2>&1`;

    warn($cmd);

    die($output) if $output ne "";


    system("mkdir -p $self->{doc_root}/.snapshots");
    system("touch $self->{doc_root}/.snapshots/$latest");
}


sub uptodate
{
    my $self = shift;

    my $latest = $self->get_latest();

    if( !defined $latest || -e $self->{doc_root}."/.snapshots/".$latest.".brackup" ){
        print "Upto date\n";
        return 0;
    }

    print "Needs update\n";

    return 1;
}

1;
