#
#
#

package Spread::Connection;

use strict;
use warnings;
use Spread;
use Data::Dumper;

our $VERSION = '0.1';

sub new
{
    my ($class, $name) = @_;
    my $self = bless {
        callbacks => undef,
    }, $class;

    ($self->{mbox}, $self->{private_group}) = Spread::connect ({
            spread_name => '4803@localhost',
            private_name => $name,
        });

    return $self;
}

sub DESTROY
{
    my ($self) = @_;
    $self->_leave (keys %{$self->{groups}});
    Spread::disconnect ($self->{mbox});
}

sub _join
{
    my ($self, @groups) = @_;
    foreach my $group (@groups)
    {
        $self->{groups}{$group} = Spread::join ($self->{mbox}, $group);
    }
}

sub _leave
{
    my ($self, @groups) = @_;
    foreach my $group (@groups)
    {
        $self->{groups}{$group} = Spread::leave ($self->{mbox}, $group);
    }
}

# subscribing to everything is not really possible, we'll only listen to groups
# we've seen explicitly in a group param here, or joined otherwise so keep that 
# in mind
sub subscribe
{
    my ($self, $sender, $group, $mess_type, $callback) = @_;

    $sender = '' unless (defined $sender);
    $group = '' unless (defined $group);
    $mess_type = '' unless (defined $mess_type);

    # make sure we're subscribed to the group in question
    $self->_join ($group) unless ($self->{groups}{$group});
    push @{$self->{callbacks}{$sender}{$group}{$mess_type}}, $callback;
}

sub queue
{
    my ($self, $group, $mess_type, $message) = @_;
    push @{$self->{pending_messages}}, {
        group => $group,
        mess_type => $mess_type,
        message => $message,
    };
}

# -1 means don't wait for any. 0 means go forever, > 0 means receive that many
sub run
{
    my ($self, $count) = @_;

    # send out any pending messages
    $self->_drain;

    my ($sender, $groups, $mess_type, $message);
    my $i = 0;
    while (not $count or $i < $count)
    {
        # look and see if we have anything waiting for us, don't stay too long
        # come back out so we can drain and look again
        (undef, $sender, $groups, $mess_type, undef, 
            $message) = Spread::receive ($self->{mbox}, 0.5);
        if ($mess_type and $mess_type > 0)
        {
            $i++;
            foreach my $group (@$groups)
            {
                $self->_dispatch ($sender, $group, $mess_type, $message);
            }
        }

        # send out any pending messages
        $self->_drain;
    }
}

sub _drain
{
    my ($self) = @_;

    my $message;
    while ($message = shift (@{$self->{pending_messages}}))
    {
        Spread::multicast ($self->{mbox}, AGREED_MESS, $message->{group}, 
            $message->{mess_type}, $message->{message});
    }
}

sub _dispatch_callbacks
{
    my ($self, $callbacks, $sender, $group, $mess_type, $message) = @_;
    if ($callbacks)
    {
        for (my $i = 0; $i < scalar (@$callbacks); $i++)
        {
            my $ret = $callbacks->[$i]->($self, $sender, $group, $mess_type, 
                $message);
            # this is kinda ugly, unless a callback returns true, we'll remove
            # it from the list
            unless ($ret)
            {
                delete $callbacks->[$i];
                $i--;
            }
        }
        # TODO: under some circumstances we want to unsubscribe here, what are
        # they
    }
}

sub _dispatch 
{
    my ($self, $sender, $group, $mess_type, $message) = @_;

    # if we haven't installed any callbacks, there's no reason to continue
    return unless ($self->{callbacks});
    # don't dispatch things we sent out
    return if (($sender eq $self->{private_group}) and 
        not $self->{see_self});

    # callbacks that match everything
    $self->_dispatch_callbacks ($self->{callbacks}{$sender}{$group}{$mess_type},
        $sender, $group, $mess_type, $message);
    # callbacks that match sender, group
    $self->_dispatch_callbacks ($self->{callbacks}{$sender}{$group}{''},
        $sender, $group, $mess_type, $message);

    # callbacks that match sender, mess_type
    $self->_dispatch_callbacks ($self->{callbacks}{$sender}{''}{$mess_type},
        $sender, $group, $mess_type, $message);

    # callbacks that match sender
    $self->_dispatch_callbacks ($self->{callbacks}{$sender}{''}{''},
        $sender, $group, $mess_type, $message);

    # callbacks that match group, mess_type
    $self->_dispatch_callbacks ($self->{callbacks}{''}{$group}{$mess_type},
        $sender, $group, $mess_type, $message);

    # callbacks that match group
    $self->_dispatch_callbacks ($self->{callbacks}{''}{$group}{''},
        $sender, $group, $mess_type, $message);

    # callbacks that match mess_type
    $self->_dispatch_callbacks ($self->{callbacks}{''}{''}{$mess_type},
        $sender, $group, $mess_type, $message);

    # callbacks that match nothing
    $self->_dispatch_callbacks ($self->{callbacks}{''}{''}{''},
        $sender, $group, $mess_type, $message);
}

1;
