#!/usr/bin/env perl
use strict; use warnings;

use Data::Dumper;
use Getopt::Long;

use EV;
use AnyEvent;
use AnyEvent::MySQL;

my $cv = AE::cv;

my $opt = {
  host => '127.0.0.1',
  port => 33060,
  user => 'crawler',
  database => 'artfake',
};

GetOptions ($opt,
  'host=s',
  'port=i',
  'user=s',
  'database=s',
  'password-file=s',
) or die "Invalid command line\n";

die "want --password-file=<path/to//mysql-test-user-passwd.txt>" unless $opt->{'password-file'};
stat $opt->{'password-file'};
if (!-f _ || ! -r _ ) {
  die sprintf('--password-file should point to readable file, "%s" is given', $opt->{'password-file'});
}

open my $f, '<', $opt->{'password-file'} or die "password file open error: $!";
$opt->{password} = <$f>;
close $f;
chomp $opt->{password};
die "password should not be empty" unless length($opt->{password}) > 0;


my $data_source = sprintf('DBI:mysql:database=%s;host=%s;port=%d', @{$opt}{qw(database host port)});

my $DB; $DB = AnyEvent::MySQL->connect($data_source, @{$opt}{qw(user password)}, {Verbose => 0, PrintError => 1 }, sub {
  return unless $DB;
  my $dbh = shift;
  if ($dbh) {
    warn "Connect Success\n";
    $dbh->do('replace into arts (id,author, title, hash) values (1, "Автор", "Название", "deadbeef")', sub {
      my $rv = shift;
      if (defined $rv) {
        warn "Do success: $rv";
      } else {
        warn "Do fail: $AnyEvent::MySQL::errstr ($AnyEvent::MySQL::err)";
      }
      $cv->send;
    });
    #$cv->send;
  } else {
    warn "Connect fail: $AnyEvent::MySQL::errstr ($AnyEvent::MySQL::err)";
    $cv->send;
  }
  undef $DB if $DB;
});

$cv->recv;

