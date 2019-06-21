#!/usr/bin/env perl
use strict;
use warnings;
use utf8;

# ------------------
package Local::Queue;

use Mouse;

has _tasks => ( is => 'rw', default => sub {
  [
    { url => 'https://readli.net/' },
    map { {url => "https://readli.net/page/$_/"} } (2..5 ) ,
  ]
} );

sub nextTask { return shift @{$_[0]->{_tasks}} };
sub length { return scalar @{$_[0]->{_tasks}} };

__PACKAGE__->meta->make_immutable();

# ------------------
package main;


use Data::Dumper;

use EV;
use AnyEvent;
use AnyEvent::HTTP;
use Time::HiRes qw(time);

#use JSON::XS;
#our $JSON=JSON::XS->new->utf8;


$AnyEvent::HTTP::USERAGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36';

my $queue = Local::Queue->new();

warn sprintf("[%.6f] gonna GET %d urls\n", time, $queue->length);
my $cv = AE::cv { EV::unloop }; $cv->begin;
while ( my $task = $queue->nextTask() ) {
  $cv->begin;
  $task->{_started} = time;
  warn sprintf("[%.6f] go GET %s\n", $task->{_started}, $task->{url});
  http_request GET => $task->{url}, sub {
    my ($body, $headers) = (shift, shift);
    $task->{_finished} = time;
    $task->{_duration} = $task->{_finished} - $task->{_started};
    my $ident = sprintf("[%.6f][+%.3fs][GET %s]", $task->{_finished}, $task->{_duration}, $task->{url});
    if ($headers->{Status} == 200) {
      warn sprintf("%s request OK %s\n", $ident, substr($body,0,15));
    } else {
      warn sprintf("%s request FAIL %d\n", $task->{url}, $headers->{Status});
    }
    $cv->end;
  };
}

$cv->end;

EV::loop;
warn sprintf("[%.6f] Exiting...)\n", time);

