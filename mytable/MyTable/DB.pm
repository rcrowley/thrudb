#
#
#

# a shared connection and set of prepared statements
package MyTable::DB::Connection;

use strict;
use warnings;
use Data::Dumper;

sub new
{
    my $class = shift @_;
    #print STDERR "** new MyTable::DB::Connection ($class, @_)\n";

    return bless {
        _dbh => shift @_,
        _stmts => {},
    }, $class;
}

sub DESTROY
{
    my ($self) = @_;
    #print STDERR "** MyTable::DB::Connection->DESTROY ($self)\n";
    foreach my $tbl (values %{$self->stmts})
    {
        foreach my $stmt (values %$tbl)
        {
            $stmt->finish if ($stmt);
        }
    }
    $self->dbh->disconnect;
}

sub dbh
{
    return $_[0]->{_dbh};
}

sub stmts
{
    return $_[0]->{_stmts};
}


1;

package MyTable::DB;

use strict;
use warnings;
use Data::Dumper;

use DBI;

our %connections = ();

sub new
{
    my $class = shift @_;
    #print STDERR "** new MyTable::DB ($class, @_)\n";

    my $args = {@_};

    my $self = bless {}, $class;

    my $host = $args->{host} || 'localhost';
    my $db = $args->{db} || 'mytable';
    my $dsn = 'DBI:mysql:database='.$db.';host='.$host;
    my $user = 'root';
    my $pass = 'mysqlpass';

    $self->{dsn} = $dsn;

    # share a pool of db connections
    my $conn;
    unless ($connections{$dsn})
    {
        print STDERR "opening connection: $dsn\n";
        my $dbh = DBI->connect ($dsn, $user, $pass);
        $connections{$dsn} = new MyTable::DB::Connection ($dbh);
    }
    $self->{conn} = $connections{$dsn};

    return $self;
}

sub prepare
{
    my ($self, $tbl, $name, $statement) = @_;
    unless ($self->{conn}->stmts->{$name})
    {
        $self->{conn}->stmts->{$tbl}{$name} = $self->{conn}->dbh->prepare ($statement);
    }
}

sub execute
{
    my ($self, $tbl, $name, @binds) = @_;
    #print STDERR "** MyTable::DB->execute ($self, $name, @binds)\n";
    return $self->{conn}->stmts->{$tbl}{$name}->execute (@binds);
}

sub fetchrow_hashref
{
    my ($self, $tbl, $name) = @_;
    return $self->{conn}->stmts->{$tbl}{$name}->fetchrow_hashref;
}

sub fetchall_arrayref
{
    my ($self, $tbl, $name) = @_;
    return $self->{conn}->stmts->{$tbl}{$name}->fetchall_arrayref;
}

sub do
{
    my ($self, $statement, @binds) = @_;
    return $self->{conn}->dbh->do ($statement, undef, @binds);
}

sub selectrow_hashref
{
    my ($self, $statement) = @_;
    return $self->{conn}->dbh->selectrow_hashref ($statement);
}

sub selectall_arrayref
{
    my ($self, $statement) = @_;
    return $self->{conn}->dbh->selectall_arrayref ($statement);
}

sub selectall_hashref
{
    my ($self, $statement, $key_field) = @_;
    return $self->{conn}->dbh->selectall_hashref ($statement, $key_field);
}

sub last_insert_id
{
    my ($self) = @_;
    return $self->{conn}->dbh->{mysql_insertid};
}

1;
