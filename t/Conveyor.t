BEGIN {				# Magic Perl CORE pragma
    if ($ENV{PERL_CORE}) {
        chdir 't' if -d 't';
        @INC = '../lib';
    }
}

use strict;
use warnings;
use Test::More tests => 2 + (2 * (16 + 3 * (4 * 5 + 3 * 6 + 1) ) );

BEGIN { use_ok('Thread::Conveyor') }

my $default_optimize = $] > 5.008 ? 'cpu' : 'memory';
is( Thread::Conveyor->optimize,$default_optimize,"Check default optimization" );

foreach my $optimize (qw(cpu memory)) {

  diag( "test belt optimized for $optimize" );

  my @base = (optimize => $optimize);

  my $belt = Thread::Conveyor->new( {@base} );
  isa_ok( $belt, 'Thread::Conveyor', 'check object type' );

  can_ok( $belt,qw(
   clean
   clean_dontwait
   maxboxes
   minboxes
   new
   onbelt
   peek
   peek_dontwait
   put
   take
   take_dontwait
   shutdown
   thread
   tid
  ) );

  $belt->put( qw(a b c) );
  $belt->put( [qw(a b c)] );
  $belt->put( {a => 1, b => 2, c => 3} );

  is( $belt->onbelt, 3,			'check number boxes on belt');

  my @l = $belt->take;
  is( @l, 3,				'check # elements simple list' );
  ok( ($l[0] eq 'a' and $l[1] eq 'b' and $l[2] eq 'c'), 'check simple list' );

  my @lr = $belt->take_dontwait;
  cmp_ok( @lr, '==', 1,			'check # elements list ref' );
  is( ref($lr[0]), 'ARRAY',		'check type of list ref' );
  ok( ($lr[0]->[0] eq 'a' and $lr[0]->[1] eq 'b' and $lr[0]->[2] eq 'c'),
   'check list ref'
  );

  my @hr = $belt->peek_dontwait;
  cmp_ok( @hr, '==', 1,			'check # elements hash ref, #1' );
  is( ref($hr[0]), 'HASH',		'check type of hash ref, #1' );

  @hr = $belt->peek;
  cmp_ok( @hr, '==', 1,			'check # elements hash ref, #2' );
  is( ref($hr[0]), 'HASH',		'check type of hash ref, #2' );

  @hr = $belt->take;
  cmp_ok( @hr, '==', 1,			'check # elements hash ref, #3' );
  is( ref($hr[0]), 'HASH',		'check type of hash ref, #3' );
  ok( ($hr[0]->{a} == 1 and $hr[0]->{b} == 2 and $hr[0]->{c} == 3),
   'check hash ref'
  );

  my @e = $belt->take_dontwait;
  cmp_ok( @e, '==', 0,			'check # elements dontwait' );
  $belt->shutdown;

  foreach my $times (10,1000,100000) {

      my @n : shared = ();
      my $belt;
    foreach (
     {@base},
     {@base, maxboxes => undef},
     {@base, maxboxes => 500, minboxes => 495},
        {@base, maxboxes => 500, minboxes => 495, threadf => sub {
            while (1) {
                my @g = $belt->clean;
                unless (defined( $g[-1][0] )) {
                    push @n, map { $_->[0] } @g[0..(@g-2)];
                    last;
                }
                push @n, map { $_->[0] } @g;
       }
         } },
    ) {

      my %saved = (%$_);
      $belt = Thread::Conveyor->new( $_ );

      isa_ok( $belt,'Thread::Conveyor',	'check object type' );

      @n = ();

      my $thread = threads->new( $saved{'threadf'} // sub {
       while (1) {
         my ($n) = $belt->take;
         last unless defined( $n );
         push( @n,$n );
       }
      } );
      isa_ok( $thread,'threads',		'check object type' );


      my $mb = eval { $belt->maxboxes };
      if ($@) {
          $mb = $times;
          ok (! defined $saved{'maxboxes'}, 'maxboxes weren\'t defined if it throws');
      } else {
          cmp_ok ($mb, '==', $saved{'maxboxes'} // 50, 'check the preset number of maxboxes');
          $belt->maxboxes($mb+2);
          cmp_ok ($belt->maxboxes, '==', $mb+2, 'check setting maxboxes');

          threads->new(sub { $belt->maxboxes($mb+1); })->join;
          cmp_ok ($belt->maxboxes, '==', $mb+1, 'check setting maxboxes from a thread');

          $belt->minboxes($mb-2);
          cmp_ok ($belt->minboxes, '==', $mb-2, 'check setting minboxes');

          threads->new(sub { $belt->minboxes($mb-1); })->join;
          cmp_ok ($belt->minboxes, '==', $mb-1, 'check setting minboxes from a thread');

          $belt->maxboxes($mb);
          cmp_ok ($belt->minboxes, '==', $mb/2, 'check that maxboxes sets minboxes');
      }
      my $maxboxes_ok = 0;
      foreach ((1..$times),undef) {
          $belt->put( $_ );
          $maxboxes_ok++ if $mb>=$belt->onbelt;
      }
      ok( !defined( $thread->join ),	'check result of join()' );
      ok( $maxboxes_ok > 0.9*$times, 'throttling works OK' );

      my $check = '';
      $check .= $_ foreach 1..$times;
      is( join('',@n),$check,		'check result of boxes on belt' );
    }
  }
}
