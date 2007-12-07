#
#
#

package MyTable::DB;

use strict;
use warnings;
use Data::Dumper;

use DBI;

sub new
{
    my $class = shift @_;
    #print STDERR "** MyTable::DB->new ($class, @_)\n";

    my $args = {@_};

    my $self = bless {}, $class;

    my $host = $args->{host} || 'localhost';
    my $db = $args->{db} || 'mytable';
    my $dsn = 'DBI:mysql:database='.$db.';host='.$host;
    my $user = 'root';
    my $pass = 'mysqlpass';
        
    $self->{dsn} = $dsn;
    $self->{dbh} = DBI->connect ($dsn, $user, $pass);

    return $self;
}

sub DESTROY
{
    my ($self) = @_;
    #print STDERR "** MyTable::DB->DESTROY ($self)\n";
    $_->finish foreach (values %{$self->{stmts}});
    $self->{dbh}->disconnect;
}

sub prepare
{
    my ($self, $name, $statement) = @_;
    $self->{stmts}{$name} = $self->{dbh}->prepare ($statement);
}

sub execute
{
    my ($self, $name, @binds) = @_;
    #print STDERR "** MyTable::DB->execute ($self, $name, @binds)\n";
    return $self->{stmts}{$name}->execute (@binds);
}

sub fetchrow_hashref
{
    my ($self, $name) = @_;
    return $self->{stmts}{$name}->fetchrow_hashref;
}

sub do
{
    my ($self, $statement, @binds) = @_;
    return $self->{dbh}->do ($statement, undef, @binds);
}

sub selectrow_hashref
{
    my ($self, $statement) = @_;
    return $self->{dbh}->selectrow_hashref ($statement);
}

sub selectall_arrayref
{
    my ($self, $statement) = @_;
    return $self->{dbh}->selectall_arrayref ($statement);
}

sub selectall_hashref
{
    my ($self, $statement, $key_field) = @_;
    return $self->{dbh}->selectall_hashref ($statement, $key_field);
}

sub last_insert_id
{
    my ($self) = @_;
    return $self->{dbh}->{mysql_insertid};
}

1;
