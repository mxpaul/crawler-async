#!/usr/bin/env perl
use strict;
use warnings;
use utf8;

# ----------------------
package Local::ArtStorage;

use Mouse;
use Carp;
use AnyEvent::MySQL;

has dbh => ( is => 'rw', );
has host => ( is => 'rw', required => 1);
has port => ( is => 'rw', required => 1);
has user => ( is => 'rw', required => 1);
has password => ( is => 'rw', required => 1);
has database => ( is => 'rw', required => 1);

has on_connect => (is => 'rw', );
has on_disconnect => (is => 'rw', );

has ready => ( is => 'rw' );

sub data_source { my $self = shift;
  return sprintf('DBI:mysql:database=%s;host=%s;port=%d', @{$self}{qw(database host port)});
}

sub connect { my $self = shift;
  croak "protect from double connect call" if $self->dbh;
  $self->{ready} = 0;
  $self->{dbh} = AnyEvent::MySQL->connect(
    $self->data_source,
    $self->user,
    $self->password,
    { PrintError => 1, Verbose => 1},
    sub { my $dbh = shift;
      if (defined $dbh) {
        $self->{ready} = 1;
        $self->{on_connect}->($dbh) if ref $self->{on_connect} eq 'CODE';
      } else {
        $self->{ready} = 0;
        $self->{on_disconnect}->() if ref $self->{on_disconnect} eq 'CODE';
      }
    },
  );
}

sub upsert_art { my $self = shift;
  my $cb = pop or croak 'want cb';
  my $art = shift;
  my $ctx = shift;

  if (! $self->{ready}) {
    $cb->({error => 'mysql not ready'});
    return;
  }

  $ctx->debug('enter upsert_art');
  croak 'broken art' if ! defined $art->{id};
  my @sql_param = ($art->{id},
    (map { my $s = $art->{$_}//''; utf8::encode($s) if utf8::is_utf8($s); $s} qw(author title hash))
  );
  $self->{dbh}->do("replace into arts(id,author, title, hash) values (?,?,?,?)", {},
    @sql_param,
    sub { my $rv = shift;
      if (defined $rv) {
        $cb->({error => 0, fatal => 0});
      } else {
        $cb->({
          error => "upsert_art replace failed: $AnyEvent::MySQL::errstr ($AnyEvent::MySQL::err)",
          fatal => 0
        });
      }
    }
  );

}

# ----------------------
package Local::Ctx;

sub cur_rss {
  do { local $/; my $f; open($f, '<', "/proc/$$/stat") ? (split /\s/, <$f>)[23] : 0; };
}
sub rss_grow { my $self = shift; $self->{start_rrs} - cur_rss; }

use Mouse;
use Time::HiRes qw(time);

our $DEBUG = 1;

has queue => ( is => 'rw', required => 1);
has stat => ( is => 'rw', required => 1);
has artstor => ( is => 'rw', required => 1);
has ident => (is => 'rw', default => '' );
has created => (is => 'rw', default => sub { time } );
has start_rrs => (is => 'rw', default => sub { cur_rss; } );
has last_log_ts => (is => 'rw', default => sub { time } );

sub update_last_log_ts { my $self = shift;
  my $ts = $self->{last_log_ts};
  $self->{last_log_ts} = time;
  return time - $ts;
}

sub log_msg { my $self = shift;
  my $lvl = shift;
  my $msg = sprintf("[%s][%.6f][+%.3fs][%db/%db][%s] %s\n",
    $lvl,
    time,
    $self->update_last_log_ts,
    $self->cur_rss,
    $self->rss_grow,
    $self->ident,
    join(" ", @_),
  );
  utf8::encode $msg;
  warn $msg;
}

sub warn { my $self = shift; $self->log_msg('WRN', @_); }
sub error { my $self = shift; $self->log_msg('ERR', @_); }
sub info { my $self = shift; $self->log_msg('INF', @_); }
sub debug { my $self = shift; $self->log_msg('DBG', @_) if $DEBUG; }


__PACKAGE__->meta->make_immutable();
# ----------------------
package Local::Stat;

use Mouse;

has _stat => ( is => 'rw', default => sub { {}; } );

sub add { my $self = shift;
  my ($key, $value) = @_;
  $self->{_stat}{$key} += $value//1;
};

sub asString { my $self = shift;
  return join('; ',
    (map { join(': ', $_, $self->{_stat}{$_}) } sort keys %{$self->{_stat}})
  );
};


__PACKAGE__->meta->make_immutable();
# ----------------------
package Local::Queue;

use Mouse;

has _tasks => ( is => 'rw', default => sub { [ ]; } );

sub nextTask { return shift @{$_[0]->{_tasks}} };
sub length { return scalar @{$_[0]->{_tasks}} };

sub push { my $self = shift;
  my $task = shift;
  push @{ $self->{_tasks} }, $task;
};

__PACKAGE__->meta->make_immutable();
# ----------------------
package Local::LitRes::ListingPage;

use Mouse;
use Carp;
use AnyEvent::HTTP;
use XML::LibXML;

sub page_url {
  my $page_id = shift;
  return sprintf("https://www.litres.ru/novie/page-%d/?lite=1", $page_id);
}

sub work { my $self = shift;
  my $caller_cb = pop or croak 'want callback';
  my $task = shift; ref $task eq 'HASH' or croak 'want task hash';
  my $ctx = shift or croak 'want ctx';
  $ctx->ident(sprintf("pagelist %.6d", $task->{page_id}));
  my $url = page_url($task->{page_id});
  $ctx->debug("go GET", $url);
  $ctx->stat->add('task_page_start');
  my $cb = sub { $ctx->stat->add('task_page_stop'); goto &$caller_cb};
  my $resp = {error => 0, fatal => 0};
  http_request(GET => $url, sub {
    my ($body, $headers) = (shift, shift);
    #my $ident = sprintf("[%.6f][+%.3fs][GET %s]", $self->{_finished}, $self->{_duration}, $self->task->{url});
    if ($headers->{Status} == 200) {
      $ctx->debug("request OK " . substr($body,0,15));

      my $doc = eval{ XML::LibXML->load_html(string => $body, 'recover' => 1, suppress_errors => 1)};
      if ($@) {
        $resp->{error} = sprintf("HTML parse error %s", $@);
        $cb->($resp);
        return;
      }

      my @booklinks = eval { $doc->findnodes('//div[@class="art__name"]/a/@href')->to_literal_list(); };
      if ($@) {
        $resp->{error} = sprintf("XPath error %s", $@);
        $cb->($resp);
        return;
      }

      for my $link (@booklinks) {
        $ctx->stat->add('art_links_found');
        #$ctx->debug(sprintf("book link %s", $link));
        $ctx->queue->push({
          type => 'Local::LitRes::ArtPage',
          url => sprintf('https://www.litres.ru%s', $link),
        });
      }
      if ( scalar(@booklinks) > 0 ) {
        $ctx->queue->push({
          type => 'Local::LitRes::ListingPage',
          page_id => $task->{page_id} + 1,
        });
      }
      $ctx->stat->add('task_page_ok');
      $cb->($resp);
    } else {
      $ctx->stat->add('task_page_fail');
      my $msg = sprintf("%s request FAIL %d", $url, $headers->{Status});
      $ctx->error($msg);
      @{$resp}{qw(error fatal)} = ($msg, $headers->{Status} < 500 ? 1 : 0);
      $cb->($resp);
    }
  });
}

__PACKAGE__->meta->make_immutable();
# ----------------------
package Local::LitRes::ArtPage;

use Mouse;
use Carp;
use AnyEvent::HTTP;
use XML::LibXML;
use Data::Dumper;
use Digest::MD5 qw(md5_hex);

sub work { my $self = shift;
  my $caller_cb = pop or croak 'want callback';
  my $task = shift; ref $task eq 'HASH' or croak 'want task hash';
  my $ctx = shift or croak 'want ctx';
  $ctx->ident(sprintf("page %s", $task->{url}));
  my $url = $task->{url};
  $ctx->debug("go GET", $url);
  my $resp = {error => 0, fatal => 0};
  $ctx->stat->add('task_art_start');
  my $cb = sub { $ctx->stat->add('task_art_stop'); goto &$caller_cb };

  $self->fetchXMLdoc($url, $ctx, sub {
    my $res = shift;
    if ( $res->{error} ) {
      $ctx->stat->add('task_art_fail');
      $cb->($resp);
    } else {
      my ($match, $err) = $self->grep_art_data($res->{tree}, $ctx);
      if ($err) {
        $ctx->stat->add('task_art_fail');
        $resp->{error} = $err;
        $cb->($resp);
        return;
      }

      my $art = $self->art_data_to_art($match, $ctx);
      my $art_log_ident = sprintf('[%s] A="%s" T="%s"',
        map { $art->{$_}//'None' } qw(id author title)
      );
      if ($art->{link}) {
        $ctx->stat->add('art_with_link');
        $self->get_demo_hash($art->{link}, $ctx, sub {
          my $res = shift;
          if ($res->{error}) {
            $ctx->stat->add('task_art_hash_fail');
            $resp->{error} = sprintf('error get demo hash %s url=[%s]: %s', $art_log_ident, $res->{error});
            $ctx->error($resp->{error});
            $cb->($resp);
          } else {

            $art->{hash} = $res->{hash};

            $ctx->stat->add('task_art_ok');
            $ctx->stat->add('task_art_hash_ok');
            $ctx->info(sprintf('art hash found %s HASH="%s" DL=%s', $art_log_ident,
              map { $art->{$_}//'None' } qw(hash link),
            ));

            $ctx->artstor->upsert_art($art, $ctx, sub { # Store art
              my $res = shift;
              if ($res->{error}) {
                $resp->{error} = sprintf('error save art to storage: %s', $res->{error});
                $ctx->error($resp->{error});
                $cb->($resp);
              } else {
                $ctx->info(sprintf('art hash saved %s HASH="%s" DL=%s', $art_log_ident,
                  map { $art->{$_}//'None' } qw(hash link),
                ));
                $cb->($resp);
              }
            });
          }
        });
      } else {
        $ctx->stat->add('task_art_ok');
        $ctx->stat->add('art_nolink');
        $ctx->artstor->upsert_art($art, $ctx, sub { # Store art
          my $res = shift;
          if ($res->{error}) {
            $resp->{error} = sprintf('error save art to storage: %s', $res->{error});
            $ctx->error($resp->{error});
            $cb->($resp);
          } else {
            $ctx->info(sprintf('art saved %s', $art_log_ident,));
            $cb->($resp);
          }
        });
      }
    }
  });
}

sub art_data_to_art { my $self = shift;
  my $match = shift or croak 'want art grep match hash';
  my $ctx = shift or croak 'want context';
  my $art = {
    (map { $_ => $match->{$_}} qw(author title id) ),
    (is_audio => $match->{media_format} eq 'Аудио' ? 1 : 0),
  };
  if (! $match->{preorder}) {
    if ($match->{media_format} eq "Аудио") {
      $ctx->stat->add('art_audio');
      $ctx->debug(sprintf('found audio art: [%s] A="%s" T="%s" Format="%s"',
        map { $art->{$_}//'None' } qw(id author title ),
        $match->{media_format},
      ));
    } else {
      my $format = '';
      if ($match->{media_format} eq 'Текст') {
        $format = 'fb2.zip';
        $ctx->stat->add('art_text');
      } elsif ($match->{media_format} eq 'PDF') {
        #$format = 'pdf';
        $ctx->stat->add('art_pdf');
      } else {
        $ctx->stat->add('art_other');
      }
      if ($format) {
        $art->{link} = sprintf("https://www.litres.ru/gettrial/?art=%d&format=%s&lfrom=236997940",
          $match->{id},
          $format,
        );
      }
      $ctx->debug(sprintf('found downloadable art: [%s] A="%s" T="%s" DL="%s"',
        map { $art->{$_}//'None' } qw(id author title link),
      ));
    }
  } else {
    $ctx->stat->add('art_preorder');
    $ctx->debug(sprintf('found missing art: [%s] A="%s" T="%s" Format="%s"',
      map { $art->{$_}//'None' } qw(id author title ),
      $match->{media_format},
    ));
  }
  return $art;
}

sub fetch { my $self = shift;
  my $cb = pop or croak 'want callback';
  my $url = shift or croak 'want url';
  my $ctx = shift or croak 'want ctx';

  http_request(GET => $url, sub {
    my ($body, $headers) = (shift, shift);
    if ($headers->{Status} == 200) {
      $ctx->debug(sprintf("request OK %s", $url));
      $cb->({body => $body, error => ''});
    } else {
      my $msg = sprintf("request FAIL [%s]:  %d %s", $url, @{$headers}{qw(Status Reason)});
      my $fatal = $headers->{Status} < 500 ? 1 : 0;
      $cb->({error => $msg, fatal => $fatal});
    }
  });
}

sub fetchXMLdoc { my $self = shift;
  my $cb = pop or croak 'want callback';
  my $url = shift or croak 'want url';
  my $ctx = shift or croak 'want ctx';
  $self->fetch($url, $ctx, sub {
    my $res = shift;
    if ($res->{error}) {
      $cb->($res);
    } else {
      my $doc = eval{ XML::LibXML->load_html(string => $res->{body}, 'recover' => 1, suppress_errors => 1)};
      if ($@) {
        $cb->({fatal => 1, error => sprintf("HTML parse error for url [%s] %s", $url, $@)});
      } else {
        $cb->({tree => $doc});
      }
    }
  });
}

sub get_demo_hash { my $self = shift;
  my $cb = pop or croak 'want callback';
  my $url = shift or croak 'want url';
  my $ctx = shift or croak 'want ctx';
  $self->fetch($url, $ctx, sub {
    my $res = shift;
    if ($res->{error}) {
      $cb->($res);
    } else {
      $cb->({ hash => md5_hex($res->{body})});
    }
  });
}

sub grep_art_data { my $self = shift;
  my $tree = shift or croak 'want tree';
  my $ctx = shift or croak 'want ctx';
  my $xpath = {
    title => '//div[contains(@class,"biblio_book_name")]/h1/text()',
    media_format => '//div[contains(@class,"biblio_book_name")]/h1/span/text()',
    author => '//a[contains(@class,"biblio_book_author__link")]/text()',
    id => '//div[@class="biblio_book_rating"]/div[@class="rating"]/div/@data-id',
    preorder => '//div[contains(@class,"biblio_book_text_preorder_info")]/text()',
  };

  my %match;
  for my $key (keys %$xpath) {
    ($match{$key}) = eval { $tree->findnodes($xpath->{$key})->to_literal_list(); };
    if ($@) {
      return (undef, sprintf("%s XPath error %s", $key, $@));
    }
    $match{$key} =~ s/(?:^\s+|\s+$)//sg if defined $match{$key};
  }
  return (\%match, undef);
}

__PACKAGE__->meta->make_immutable();
# ----------------------
package main;


use Data::Dumper;

use EV;
use AnyEvent;
use AnyEvent::HTTP;
use Time::HiRes qw(time);
use Getopt::Long;

#use JSON::XS;
#our $JSON=JSON::XS->new->utf8;

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


$AnyEvent::HTTP::USERAGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36';

my $queue = Local::Queue->new();
# Last page 10764
$queue->push(
  {type => 'Local::LitRes::ListingPage', page_id => 1},
  #{type => 'Local::LitRes::ListingPage', page_id => 10766},
  ##{type => 'Local::LitRes::ArtPage', url => 'https://www.litres.ru/pavel-ardashev/peterburgskie-otgoloski/'},
  ##{type => 'Local::LitRes::ArtPage', url => 'https://www.litres.ru/maks-fray/tyazhelyy-svet-kurteyna-zheltyy/'},
  ##{type => 'Local::LitRes::ArtPage', url => 'https://www.litres.ru/ludmila-nevzgodina/udivitelnye-novogodnie-igrushki-i-suveniry-sozdaem-svoimi-rukami/'}, # Нет в продаже
  ##{type => 'Local::LitRes::ArtPage', url => 'https://www.litres.ru/ieromonah-serafim-rouz/sovremennoe-nedochelovechestvo/'}, # Аудио
  #{type => 'Local::LitRes::ArtPage', url => 'https://www.litres.ru/vissarion-belinskiy/nadezhda-sobranie-sochineniy-v-stihah-i-proze-izd-a-kulchickiy/'},
);


my $MAX_PAGE_FAIL_COUNT = 3;

$Local::Ctx::DEBUG = 1;

my $max_parallel = 20;
my $running = 0;
my $task_max_retry_on_fail = 2;

my $stat = Local::Stat->new();
my $storage = Local::ArtStorage->new(map { $_ => $opt->{$_}} qw(host port user password database));

my $process_queue; $process_queue = sub {
  warn sprintf("Stat: %s Running: [%d/%d]", $stat->asString, $running, $max_parallel);

  return unless $storage->ready;
  return unless $running < $max_parallel;
  my $task = $queue->nextTask();
  unless ($task) {
    EV::unloop if $running == 0;
    return;
  }

  my $type = $task->{type};
  my $ctx = Local::Ctx->new(queue => $queue, stat => $stat, artstor => $storage );
  $ctx->cur_rss;
  $stat->add('task_started');
  my $worker = $type->new(task => $task, ctx => $ctx);
  $running++;
  $worker->work($task, $ctx, sub {
    $running --;
    $stat->add('task_finished');
    my $res = shift;
    if ($res->{error}) {
      if (! $res->{fatal}) {
        $stat->add('task_retry_on_fail');
        if (++$task->{retry_count} < $task_max_retry_on_fail ) {
          $queue->push($task);
        }
      }
      warn sprintf("Error! %s", $res->{error});
      $stat->add('task_fail');
    } else {
      $stat->add('task_ok');
    }
    $process_queue->();
  });
  $process_queue->();

}; $process_queue->();


$storage->on_connect($process_queue);
$storage->connect();
EV::loop;
warn sprintf("Done. Stat: %s", $stat->asString);

