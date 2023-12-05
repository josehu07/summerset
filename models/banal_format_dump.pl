#!/usr/bin/perl -w
#
# Copyright (C) 2007 Geoffrey M. Voelker
# Copyright (c) 2016-2023 Eddie Kohler; see LICENSE.
#
# banal -- analyze pdf formatting
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# Geoffrey M. Voelker (voelker@cs.ucsd.edu)
# Eddie Kohler (ekohler@gmail.com)
#

# todo:
# -- computer modern roman fonts
# -- embedded java script, remoteapproach.com

use POSIX;
use Data::Dumper;
use File::Basename;
require File::Temp;
use List::Util qw(min max);
my($FILE, $banal_text_fudge);

sub usage {
    print <<EOF;
usage: banal [-report | -stats | -json] [-zoom=N] files

banal has three modes of operation:

-report  print full formatting info for all pages.  this mode is
         the default if no mode is specified:

         % banal paper.pdf

-stats   print formatting info condensed into one line with fields
         separated by tabs; useful for computing summary stats across
         many papers.

         fields are 'file', 'paper', 'text region', 'margins', 'font',
         'leading', 'columns', 'pages', 'app'.  for example:

         % banal -stats *.pdf | cut -f 5

         extracts font sizes from a set of pdf files.

-json     print JSON output
-pagenum  include page numbers and headings
-no-time  do not print timestamp
-debug-words debug words

-zoom=N   passed to pdftohtml

-max-pages=N  maximum number of pages for JSON

-version  report the version of banal

EOF
      exit(1);
}

# version
my($banal_version) = 1.2;

# parse args
my ($banal_mode) = "report";
my (%banal_param, $json_pagenum, $no_timestamp);
my ($debug_pdftohtml, $debug_words);
for (my $i = 0; $i < @ARGV; ) {
    if ($ARGV[$i] =~ /\A--?(stats|json|report|version)\z/) {
        $banal_mode = $1;
        splice @ARGV, $i, 1;
    } elsif ($ARGV[$i] =~ /\A--?debug[-_]pdftohtml\z/) {
        $debug_pdftohtml = 1;
        splice @ARGV, $i, 1;
    } elsif ($ARGV[$i] =~ /\A--?debug[-_]words\z/) {
        $debug_words = 1;
        splice @ARGV, $i, 1;
    } elsif ($ARGV[$i] =~ /\A--?(?:pagen(?:um|o)|headings|headers)\z/) {
        $json_pagenum = 1;
        splice @ARGV, $i, 1;
    } elsif ($ARGV[$i] =~ /\A--?(?:no-?time(?:stamp|))\z/) {
        $no_timestamp = 1;
        splice @ARGV, $i, 1;
    } elsif ($ARGV[$i] =~ /\A--?(zoom|max[-_]pages)=(.*)\z/) {
        my($name) = $1;
        $name =~ s/-/_/g;
        $banal_param{$name} = $2;
        splice @ARGV, $i, 1;
    } elsif ($ARGV[$i] =~ /\A--?(zoom|max[-_]pages)\z/ && $i + 1 < @ARGV) {
        my($name) = $1;
        $name =~ s/-/_/g;
        $banal_param{$name} = $ARGV[$i + 1];
        splice @ARGV, $i, 2;
    } elsif ($ARGV[$i] =~ /\A-/) {
        print STDERR "banal: bad option ", $ARGV[$i], "\n";
        usage;
    } else {
        $i += 1;
    }
}

my(@switches);
push @switches, "-zoom=" . $banal_param{zoom} if defined $banal_param{zoom};

# zoom value
if (exists $banal_param{zoom} && ($banal_param{zoom} !~ /^[1-9]\d*(\.\d*)?$/)) {
    print STDERR "banal: bad -zoom\n";
    usage;
}

# compensation factor for pdftohtml font size
# (commit cb5bece4f...: “pdftohtml: Don't substract -2 to font size without any reason”)
my $p2h_font_size_compensation;

# pdftohtml dimensions are scaled by this much to get decipoints
my $p2h_scale;

sub pdf2in ($) {
    return $_[0] / 720;
}

sub in2pdf ($) {
    return $_[0] * 720;
}

sub pdf2pt ($) {
    return $_[0] / 10;
}

sub pt2pdf ($) {
    return $_[0] * 10;
}

sub rpdf ($) {
    return int($_[0] * $p2h_scale + 0.5);
}

sub rpdffont ($) {
    return int(($_[0] + $p2h_font_size_compensation) * $p2h_scale + 0.5);
}

# minimum amount of text on page for it to be interesting
my $banal_min_nchars = 800;

# fudge factor when judging text regions (in inches).
$banal_text_fudge = 0.05;

# policy to use to estimate leading
my $banal_leading_policy;

# round margins and text blocks to this number of points
my $grid = 4;

# pdftohtml executable
my $pdftohtml;
if (exists $ENV{"PDFTOHTML"}) {
    $pdftohtml = $ENV{"PDFTOHTML"};
} elsif (exists $ENV{"PHP_PDFTOHTML"}) {
    $pdftohtml = $ENV{"PHP_PDFTOHTML"};
} else {
    $pdftohtml = "pdftohtml";
}

#print STDERR "using $pdftohtml...\n";

# version of pdftohtml program
my $p2h_version = 0;

# full path of file being analyzed
my $banal_fullpath = '';
# file name of file being analyzed
my $banal_filename = '';

# debug settings
my $debug_leading = 0;
my $debug_parse = 0;


# return min key in hash
sub minkey ($) {
    my ($href) = @_;
    return (sort { $a <=> $b } keys %$href)[0];
}

# return max key in hash
sub maxkey ($) {
    my ($href) = @_;
    return (sort { $a <=> $b } keys %$href)[$#_ - 1];
}

# return key of mode of values in hash
sub modevalkey ($) {
    my ($href) = @_;
    my ($modekey, $mode);
    while (my ($k, $v) = each %$href) {
        if (!defined($modekey)
            || $v > $mode
            || ($v == $mode && $k < $modekey)) {
            $modekey = $k;
            $mode = $v;
        }
    }
    $modekey;
}

# return max val in hash
sub maxval ($) {
    my ($href) = @_;
    my ($max) = (keys %$href)[0];
    map { $max = $_ if ($href->{$_} > $href->{$max}) } keys %$href;
    return $href->{$max};
}

# return 'a' == 'b'
sub bb_equal ($$) {
    my ($a, $b) = @_;
    return (($a->{top} == $b->{top}) &&
            ($a->{left} == $b->{left}) &&
            ($a->{height} == $b->{height}) &&
            ($a->{width} == $b->{width}));
}

# merge 'a' into 'b'
sub bb_merge ($$) {
    my ($a, $b) = @_;

    $b->{top} = min $a->{top}, $b->{top};
    $b->{left} = min $a->{left}, $b->{left};
    $b->{height} = max $a->{height}, $b->{height};
    $b->{width} = max $a->{width}, $b->{width};
}

sub bodyfontsize (\%\%) {
    my ($alen_bysize, $tlen_bysize) = @_;

    my ($asize) = %$alen_bysize ? modevalkey($alen_bysize) : 0;
    my ($tsize) = %$tlen_bysize ? modevalkey($tlen_bysize) : 0;
    if ($tsize && (!$asize || $alen_bysize->{$asize} < 0.6 * $tlen_bysize->{$tsize})) {
        $asize = $tsize;
    }
    $asize;
}

sub calc_page_leading ($\@$) {
    my ($page, $texts, $bodyfontsize) = @_;
    my ($minlead) = in2pdf(1/16);

    my (%leads);
    my ($colpos) = $page->{colpos};
    for (my $col = 0; $col < @$colpos; $col += 2) {
        my ($x) = POSIX::floor(0.8 * $colpos->[$col] + 0.2 * $colpos->[$col + 1]);
        my (@ypos);
        foreach my $text (@$texts) {
            if ($text->{sz} >= $bodyfontsize
                && $text->{l} <= $x
                && $text->{l} + $text->{w} >= $x) {
                push @ypos, $text->{t};
            }
        }
        @ypos = sort { $a <=> $b } @ypos;
        for (my $j = 1; $j < @ypos; ++$j) {
            my ($lead) = $ypos[$j] - $ypos[$j - 1];
            if ($lead >= $minlead) {
                $leads{$lead} = 0 if !exists $leads{$lead};
                $leads{$lead} += 1;
            }
        }
    }
    $leads{0} = 1 if !%leads;

    my ($lead) = modevalkey(\%leads);
    # $page->{lead_pt} = sprintf '%.1f',  (pdf2pt($lead) * 10 + 0.5) / 10;
    $page->{lead_pt} = int (pdf2pt($lead) * 10 + 0.5) / 10;
}

sub calc_xmap_mode_columns ($) {
    my ($xmap) = @_;

    my (%ncols);
    foreach my $z (@{$xmap->{rows}}) {
        if (@$z > 0) {
            $ncols{@$z} = 0 if !exists $ncols{@$z};
            $ncols{@$z} += 1;
        }
    }
    my ($ncols2) = modevalkey(\%ncols);
    return () if !defined($ncols2);

    # XXX should use KDE
    # find column boundaries
    my (@colpos);
    for (my $i = 0; $i < $ncols2; $i += 1) {
        push @colpos, {};
    }

    # find left edges
    foreach my $z (@{$xmap->{rows}}) {
        if (@$z == $ncols2) {
            for (my $i = 0; $i < $ncols2; $i += 2) {
                my $x = $z->[$i];
                $colpos[$i]->{$x} = 0 if !exists $colpos[$i]->{$x};
                $colpos[$i]->{$x} += 1;
            }
        }
    }
    my (@modes);
    for (my $i = 0; $i < $ncols2; $i += 2) {
        push @modes, modevalkey($colpos[$i]), 0;
    }

    # find right edges
    foreach my $z (@{$xmap->{rows}}) {
        if (@$z == $ncols2) {
            for (my $i = 0; $i < $ncols2; $i += 2) {
                my $x = $z->[$i + 1];
                if ($x > $modes[$i]) {
                    $colpos[$i + 1]->{$x} = 0 if !exists $colpos[$i + 1]->{$x};
                    $colpos[$i + 1]->{$x} += 1;
                }
            }
        }
    }
    for (my $i = 1; $i < $ncols2; $i += 2) {
        $modes[$i] = modevalkey($colpos[$i]);
    }

    my (@columns);
    for (my $i = 0; $i < $ncols2; $i += 2) {
        my ($l1, $r1) = ($modes[$i], $modes[$i + 1]);
        my ($overlap) = min(in2pdf(0.75), $r1 - $l1 - in2pdf(1/8));
        my ($l, $r, $n) = ($l1, $r1, 0);

        foreach my $z (@{$xmap->{rows}}) {
            if (@$z == $ncols2) {
                my ($l2, $r2) = ($z->[$i], $z->[$i + 1]);
                if (min($r1, $r2) - max($l1, $l2) >= $overlap) {
                    $l = min $l, $l2 if exists($colpos[$i]->{$l2})
                                        && $colpos[$i]->{$l2} > 3;
                    $r = max $r, $r2 if exists($colpos[$i + 1]->{$r2})
                                        && $colpos[$i + 1]->{$r2} > 3;
                    ++$n;
                }
            }
        }

        if ($n >= 3) {
            push @columns, $l, $r;
        }
    }
    @columns;
}

sub calc_page_columns ($$) {
    my ($page, $xmap) = @_;
    $xmap = xmap_close_gaps($xmap);
    my ($nxmap) = $xmap;

    my (@columns);
    while (1) {
        my (@ncolumns) = calc_xmap_mode_columns($nxmap);
        last if !@ncolumns;

        my ($i, $j);
        for ($i = $j = 0; $j < @ncolumns; ) {
            if ($i == @columns || $ncolumns[$j] < $columns[$i]) {
                splice @columns, $i, 0, $ncolumns[$j], $ncolumns[$j + 1];
                $nxmap = xmap_strip_overlap($nxmap, $ncolumns[$j], $ncolumns[$j + 1]);
                $j += 2;
            } else {
                $i += 2;
            }
        }

        last if xmap_count_nonempty($nxmap) < @{$nxmap->{rows}} / 16;
    }

    $page->{ncols} = @columns / 2;
    $page->{colpos} = [@columns];
    #print STDERR $page->{num}, " : ", @columns / 2, " [", join(",", @columns), "]\n";
    #xmap_print($xmap);
    #print STDERR "\n\n";
}

sub calc_page_text_region ($$$) {
    my ($page, $xmap, $ymap) = @_;

    my ($left, $right) = xmap_find_bounds($xmap, 0.1);
    my ($top, $bottom) = xmap_find_bounds($ymap, 0.1);

    $page->{textbb} = {
        top => $top,
        left => $left,
        width => $right - $left,
        height => $bottom - $top
    };

    if ($left < $right && $page->{ncols} == 0) {
        $page->{colpos} = [$left, $right];
    }
}

sub calc_page_nchars ($\%$) {
    my ($page, $tlen_bysize, $bodyfontsize) = @_;

    my ($nc) = 0;
    foreach my $size (keys %$tlen_bysize) {
        next if $size < $bodyfontsize || $size > 1.5 * $bodyfontsize;
        $nc += $tlen_bysize->{$size};
    }
    $page->{nchars} = $nc;
}

sub calc_page_nwords ($\@$) {
    my ($page, $texts, $bodyfontsize) = @_;
    my ($colpos) = $page->{colpos};
    my ($slack) = in2pdf(1/8);
    $bodyfontsize *= 0.95;

    my ($nw) = 0;
    my ($prehyphen) = undef;
    for (my $tpos = 0; $tpos < @$texts; ++$tpos) {
        my ($text) = $texts->[$tpos];
        last if $text->{is_references};
        next if $text->{sz} < $bodyfontsize;

        my ($content) = $text->{content};
        $content = $prehyphen . $content if defined($prehyphen);
        $prehyphen = undef;

        my ($tt) = $text->{t};
        my ($th) = $text->{h};
        my ($tr) = $text->{l} + $text->{w};
        while ($tpos + 1 < @$texts) {
            my $ntext = $texts->[$tpos + 1];
            if ($ntext->{l} >= $tr - $slack
                && $ntext->{l} < $tr + 3 * $slack
                && interval_overlap($tt, $th, $ntext->{t}, $ntext->{h})) {
                $content .= " " if $ntext->{l} - $tr > $text->{sz} / 6;
                $content .= $ntext->{content};
                $tr = $ntext->{l} + $ntext->{w};
                ++$tpos;
            } else {
                last;
            }
        }

        # remove references
        $content =~ s/\[[\s\d,-]+\]//g;

        if (substr($content, -1) eq "-") {
            my ($i, $r) = (1, $text->{l} + $text->{w});
            while ($i < @$colpos && $colpos->[$i] + $slack < $r) {
                $i += 2;
            }
            if ($i < @$colpos && abs($r - $colpos->[$i]) < 2 * $slack) {
                $content =~ s/([\w()\[\]\{\}\200-\377]+)-\z//;
                $prehyphen = $1;
            }
        }
        while ($content =~ /[\w\200-\377][\w\[\]\{\},.\200-\377]+(?:\(s\))?/g) {
            # print STDERR $&, "\n" if $debug_words;
            ++$nw;
        }
    }
    if (defined($prehyphen)) {
        while ($prehyphen =~ /[\w\200-\377][\w\[\]\{\},.\200-\377]+(?:\(s\))?/g) {
            # print STDERR $&, "\n" if $debug_words;
            ++$nw;
        }
    }
    $page->{nwords} = $nw;
}

sub interval_overlap ($$$$) {
    my ($a, $da, $b, $db) = @_;
    return ($a + $da >= $b && $a + $da <= $b + $db)
        || ($b + $db >= $a && $b + $db <= $a + $da);
}

sub column_gap_overlap {
    my ($text, $coll, $colr, $tt, $th, $bodyfontsize) = @_;
    my ($gap) = 0;
    if ($text->{sz} <= $bodyfontsize
        && interval_overlap($coll, $colr - $coll, $text->{l}, $text->{w})) {
        my ($slack) = $text->{sz} * 0.5;
        $gap |= 1 if $text->{t} >= $tt + $th
                && $text->{t} - ($tt + $th) < $slack;
        $gap |= 2 if $tt >= $text->{t} + $text->{h}
                && $tt - ($text->{t} + $text->{h}) < $slack;
    }
    return $gap;
}

sub line_combine {
    my ($text, $tt, $th, $tl, $tr, $slack, $isright) = @_;
    my ($textl) = $text->{l};
    my ($textr) = $textl + $text->{w};
    return interval_overlap($tt, $th, $text->{t}, $text->{h})
        && ($isright ? $textl >= $tr - $slack : $tl >= $textr - $slack)
        && ($isright ? $textl < $tr + 3 * $slack : $tl < $textr + 3 * $slack);
}

sub heading_number ($) {
    my ($t) = @_;
    if ($t =~ /\A(\d+)((?:\.\d+)*)/) {
        (+$1, +$1, $1 . $2, 1);
    } elsif ($t =~ /\A(X*)(I+|IV|VI*|IX|)((?:\.\d+)*)/ && ($1 || $2)) {
        my ($n) = length($1) * 10;
        if ($2 eq "IX") {
            $n += 9;
        } elsif ($2 eq "IV") {
            $n += 4;
        } elsif (substr($2, 0, 1) eq "V") {
            $n += 5 + length($2) - 1;
        } else {
            $n += length($2);
        }
        ($n, $1 . $2, $1 . $2 . $3, 0);
    } else {
        (0, 0, 0, 0);
    }
}

sub calc_page_headings ($$$$) {
    my ($doc, $page, $texts, $bodyfontsize) = @_;
    my ($colpos) = $page->{colpos};
    my ($slack) = in2pdf(1/8);
    my ($top) = $page->{textbb}->{top} + 0.075 * $page->{textbb}->{height};
    my ($near_top) = 1;
    my ($in_refs_top) = $doc->{in_references};
    my ($contents) = 0;
    my ($min_section) = $doc->{max_section};
    my ($max_section) = $min_section;
    my ($nskipped) = 0;

    TPOS: for (my $tpos = 0; $tpos < @$texts; ) {
        my ($tpos1) = $tpos;
        my ($text) = $texts->[$tpos];
        if ($text->{sz} < $bodyfontsize || $text->{sz} < $slack) {
            $near_top = $in_refs_top = 0 if $in_refs_top && !$text->{num} && $text->{t} > $top;
            ++$tpos;
            next;
        }

        # detect when we've moved beyond the top
        my ($tt) = $text->{t};
        my ($th) = $text->{h};
        my ($was_near_top) = $near_top;
        $near_top = 0 if $near_top && !$text->{num} && $tt + $th > $top;

        # skip unless it looks interesting
        my ($content) = $text->{content};
        if ($content !~ /\A[\dABCDEFGHIRVX]/) {
            ++$tpos;
            ++$nskipped;
            next;
        }

        # compute length of line
        my ($tl) = $text->{l};
        my ($tr) = $tl + $text->{w};
        if ($nskipped > 0
            && line_combine($texts->[$tpos-1], $tt, $th, $tl, $tr, $slack, 0)) {
            ++$tpos;
            ++$nskipped;
            next;
        }

        $nskipped = 0;
        ++$tpos;
        while ($tpos < @$texts
               && line_combine($texts->[$tpos], $tt, $th, $tl, $tr, $slack, 1)) {
            my ($textl) = $texts->[$tpos]->{l};
            $content .= " " if $textl - $tr > $text->{sz} / 6;
            $content .= $texts->[$tpos]->{content};
            $tr = $textl + $texts->[$tpos]->{w};
            ++$tpos;
        }

        # find containing column
        my ($colli) = 0;
        while ($colli < @$colpos
               && $colpos->[$colli] < $tl - $slack
               && $colpos->[$colli + 1] < $tr) {
            $colli += 2;
        }
        next if $colli == @$colpos;
        my ($colri) = $colli;
        while ($colri + 3 < @$colpos
               && $colpos->[$colri + 2] <= $tr
               && $colpos->[$colri + 1] < $colpos->[$colli] + in2pdf(1)) {
            $colri += 2;
        }
        my ($coll) = $colpos->[$colli];
        my ($colr) = $colpos->[$colri + 1];

        # skip if not left-aligned or centered
        # (“References” is a special case b/c of some fancy formats)
        if (($coll < $tl - 2 * $slack
             && abs(($tl - $coll) - ($colr - $tr)) > in2pdf(1/4)
             && (length($content) != 10 || lc($content) ne "references"))
            || ($tr >= $colr - $slack && $text->{sz} <= $bodyfontsize)) {
            next;
        }

        # check for expected headings
        $content =~ s/\s+\z//;
        if ($content !~ /\A(?:(?:X*(?:I?[XV]|V?I+))\.? +[^,;()]*|\d\d?\d?(?:\.\d+)*\.? +[A-Za-z_][^,;()]*|[ABCDEFGHJKLM](?:\.\d+)*\.? +[A-Z_][^,;()]*|(?:(?i)Abstract|Acknowledge?ments?|Contents|References?|Bibliography|Introduction|Appendices|Appendix(?:es)?[^,;()]*))\z/
            || $content =~ /[.,:]\z/) {
            next;
        }

        # skip if part of paragraph
        # (if font is body size, require that there be space before the heading)
        if ($text->{sz} <= $bodyfontsize) {
            my ($gaps) = 0;
            my ($tposl) = $tpos1 - 1;
            my ($tposr) = $tpos;
            while ($tposl >= 0 || $tposr < @$texts) {
                if ($tposl >= 0) {
                    $gaps |= column_gap_overlap($texts->[$tposl], $coll, $colr, $tt, $th, $bodyfontsize);
                    #print STDERR "kill @" . $texts->[$tposl]->{content} . " " . $texts->[$tposl]->{sz} ."($bodyfontsize) [$content]\n" if $gaps & 2;
                    --$tposl;
                }
                if ($tposr < @$texts) {
                    $gaps |= column_gap_overlap($texts->[$tposr], $coll, $colr, $tt, $th, $bodyfontsize);
                    #print STDERR "kill @" . $texts->[$tposr]->{content} . " [$content]\n" if $gaps & 2;
                    ++$tposr;
                }
                if ($gaps & 2) {
                    next TPOS;
                }
            }
        }

        # skip if already seen or unexpected
        my($hnum, $htxt, $hstxt, $hisnum) = heading_number($content);
        if ($hisnum) {
            if ($hnum > 99
                && $doc->{max_numeric_heading} > 0
                && $doc->{max_numeric_heading} < 20) {
                next TPOS;
            }
            if (exists($doc->{headings}->{$hstxt})) {
                next TPOS;
            }
            $doc->{max_numeric_heading} = $hnum if $hnum > $doc->{max_numeric_heading};
            $doc->{headings}->{$hstxt} = 1;
        }

        if (!exists($page->{headings})) {
            $page->{headings} = [];
            $page->{heading_texts} = [];
        }
        push @{$page->{headings}}, $content;
        $text->{coll} = $coll;
        $text->{colr} = $colr;
        $text->{is_references} = $content =~ /\A(?:References?|Bibliography)\z/i;
        $page->{heading1} = $text->{heading1} = 1 if $was_near_top;
        push @{$page->{heading_texts}}, $text;
        $page->{max_heading_fontsize} = $text->{sz} if $text->{sz} > $page->{max_heading_fontsize};
        if ($hnum > 0 && $hnum > $min_section) {
            $page->{first_section} = $hnum if !exists($page->{first_section}) || $page->{first_section} > $hnum;
            $max_section = $hnum if $hnum > $max_section;
        }

        # skip rest if saw contents
        if (@{$page->{headings}} == 1
            && lc($content) eq "contents") {
            last TPOS;
        }
    }

    $doc->{max_section} = $max_section;
}

sub calc_doc_text_region ($) {
    my ($doc) = @_;

    my ($maxw, $maxh, $minl, $mint);
    foreach my $i (1..$doc->{npages}) {
        my ($page) = $doc->{pages}->{$i};
        next if !exists $page->{textbb};
        next if defined($maxw)
            && (!exists $page->{nchars}
                || $page->{nchars} < $banal_min_nchars);
        my ($tbb) = $page->{textbb};
        $maxw = $tbb->{width} if !defined($maxw) || $maxw < $tbb->{width};
        $maxh = $tbb->{height} if !defined($maxh) || $maxh < $tbb->{height};
        $minl = $tbb->{left} if !defined($minl) || $minl > $tbb->{left};
        $mint = $tbb->{top} if !defined($mint) || $mint > $tbb->{top};
    }

    my ($rmarg, $bmarg);
    if (defined($maxw) && defined($minl)) {
        $rmarg = $doc->{pagebb}->{width} - ($maxw + $minl);
    } else {
        $rmarg = $minl = $maxw = 0;
    }
    if (defined($maxh) && defined($mint)) {
        $bmarg = $doc->{pagebb}->{height} - ($maxh + $mint);
    } else {
        $bmarg = $mint = $maxh = 0;
    }
    if ($rmarg < 0) {
        print STDERR "r MARGIN\n";
    }
    if ($bmarg < 0) {
        print STDERR "b MARGIN\n";
    }
    $doc->{textbb} = {
        width => $maxw,
        height => $maxh,
        left => $minl,
        top => $mint,
        rmarg => $rmarg,
        bmarg => $bmarg,
    };
}

sub version_compare ($$) {
    my ($a, $b) = @_;
    my (@a, @b);
    @a = split(/\./, $a);
    @b = split(/\./, $b);
    for ($i = 0; $i < @a && $i < @b; $i += 1) {
        my ($al) = scalar($a[$i] =~ /\A[A-Z]\z/);
        my ($bl) = scalar($b[$i] =~ /\A[A-Z]\z/);
        if ($al != $bl) {
            if ($i == 0) {
                if (substr($a[0], 0, 1) eq "A") {
                    return 1;
                } elsif (substr($b[0], 0, 1) eq "A") {
                    return -1;
                }
            }
            return -100;
        } elsif ($al ? $a[$i] lt $b[$i] : +$a[$i] < +$b[$i]) {
            return -1;
        } elsif ($al ? $a[$i] gt $b[$i] : +$a[$i] > +$b[$i]) {
            return 1;
        }
    }
    if ($i == 0) {
        return -100;
    } elsif ($i >= @a && $i < @b) {
        return -1;
    } elsif ($i < @a && $i >= @b) {
        return 1;
    } else {
        return 0;
    }
}

sub calc_doc_page_types ($) {
    my ($doc) = @_;

    my ($saw_acks_refs) = 0;
    my ($saw_appendix) = 0;
    my ($saw_conclusion) = 0;

    foreach my $i (1..$doc->{npages}) {
        my ($page) = $doc->{pages}->{$i};

        my ($see_acks_refs) = $saw_acks_refs;
        my ($see_appendix) = $saw_appendix;
        if (exists $page->{headings}) {
            my ($is_first) = exists $page->{heading1};
            foreach my $h (@{$page->{headings}}) {
                if ($h =~ /\A(?:[\dIVX]+\.? *|[ABCDE]\.? *|)(?:Acknowledge?ments?|References?|Bibliography|\AAppendices|\AAppendix(?:es)?)\z/i) {
                    $see_acks_refs = 1;
                    $saw_acks_refs = 1 if $is_first;
                    $see_appendix = 0;
                } elsif ($h =~ /\A[\dIVX]+\.? *Conclusions?(?: and future work|)\z/i) {
                    $saw_conclusion = 1;
                } elsif ($h =~ /\A(Appendix\s* |)((?:[A-M]|\d+)(?:\.\d+)*)\.?\s+(Appendices\z|Appendix(?:es|)\z|)(.*)\z/i
                         && ($see_acks_refs || $saw_conclusion)
                         && ($see_appendix
                             ? version_compare($2, $see_appendix) == 1
                             : $1 ne "" || substr($2, 0, 1) ge "A" || $3 ne "")) {
                    $see_appendix = $2;
                    $saw_appendix = $see_appendix if $saw_acks_refs
                        || ($is_first && ($1 ne "" || $saw_conclusion || $3 ne ""));
                } elsif ($h =~ /\AAppendix/i) {
                    $see_appendix = "yes";
                    $saw_appendix = $see_appendix if $saw_acks_refs || $is_first;
                } elsif ((!$see_appendix
                          || $h =~ /\A[\dIVX]+[\. ]/)
                         && (!$saw_appendix
                             || (exists($page->{bodyfontsize})
                                 && $page->{max_heading_fontsize} > $page->{bodyfontsize}))) {
                    $see_acks_refs = $saw_acks_refs = $saw_appendix = $saw_conclusion = 0;
                }
                $is_first = 0;
            }
        }

        # see CheckFormat if you change these names
        my ($type) = "body";
        if (!exists $page->{nchars} || $page->{nchars} == 0) {
            $type = "blank";
        } elsif ($i == 1 && $page->{nchars} < 800) {
            $type = "cover";
        } elsif ($saw_appendix) {
            $type = "appendix";
        } elsif ($saw_acks_refs) {
            $type = "bib";
        } elsif ($page->{nchars} < $banal_min_nchars
                 || $page->{ncols} == 0) {
            $type = "figure";
        }

        $page->{type} = $type;
        $saw_acks_refs = $see_acks_refs;
        $saw_appendix = $see_appendix;
    }
}

sub calc_doc_columns ($) {
    my ($doc) = @_;

    my (%cols, %first_page);
    foreach my $i (1..$doc->{npages}) {
        my ($page) = $doc->{pages}->{$i};
        my ($ncols) = $page->{ncols};
        if (defined ($ncols) && $ncols > 0) {
            $cols{$ncols}++;
            $first_page{$ncols} = $i if !exists $first_page{$ncols};
        }
    }

    # number of columns on greatest number of pages
    my ($ncols) = 0;
    my ($mode) = 0;
    while (my ($k, $v) = each %cols) {
        if ($v > $mode || ($v == $mode && $first_page{$k} < $first_page{$ncols})) {
            $ncols = $k;
            $mode = $v;
        }
    }
    $doc->{ncols} = $ncols;
}

sub p2h_font_bug ($) {
    my ($doc) = @_;

    return 1 if !defined($doc->{bodyfontsize}) || $doc->{bodyfontsize} <= 0;
    return 0;
}

sub p2h_serious_font_bug ($) {
    my ($doc) = @_;

    return 0 if (!p2h_font_bug ($doc));
    return 1 if ($doc->{textbb}->{width} == 0 ||
                 $doc->{textbb}->{height} == 0);
    return 0;
}

my %json_escapes = (
    "\n" => "\\n",
    "\r" => "\\r",
    "\f" => "\\f",
    "\t" => "\\t",
    "\"" => "\\\"",
    "\\" => "\\\\",
    "/" => "\\/"
);

sub json_quote ($) {
    my($x) = $_[0];
    $x =~ s{[\n\r\f\t\"\\/]}{$json_escapes{$&}}ge;
    "\"$x\"";
}

sub report_json ($) {
    my ($doc) = @_;
    binmode STDOUT, ':utf8';

    printf "{\n";
    printf "  \"at\": %d,\n", time if !$no_timestamp;
    printf "  \"args\": %s,\n", json_quote(join(" ", @switches)) if @switches;

    my $dx = {"pw" => {}, "ph" => {}, "tw" => {}, "th" => {}, "mt" => {}, "ml" => {}, "mr" => {}, "mb" => {}};
    my $px = {};
    my $nummargin = 10000;
    my $nwords = 0;
    for my $i (1 .. $doc->{npages}) {
        my $page = $doc->{pages}->{$i};
        $nwords += $page->{nwords} if exists($page->{nwords});

        my($pbb) = $page->{pagebb};
        my($pw) = POSIX::floor(pdf2pt($pbb->{width}) + 0.5);
        my($ph) = POSIX::floor(pdf2pt($pbb->{height}) + 0.5);
        my($pd) = {"pw" => $pw, "ph" => $ph};

        if (exists $page->{textbb}) {
            my($tbb) = $page->{textbb};
            my($tl) = POSIX::floor(pdf2pt($tbb->{left}) / $grid) * $grid;
            my($tt) = POSIX::floor(pdf2pt($tbb->{top}) / $grid) * $grid;
            my($tr) = POSIX::ceil(pdf2pt($tbb->{left} + $tbb->{width}) / $grid) * $grid;
            my($tb) = POSIX::ceil(pdf2pt($tbb->{top} + $tbb->{height}) / $grid) * $grid;
            $pd->{mt} = $tt;
            $pd->{ml} = $tl;
            $pd->{mb} = $ph - $tb;
            $pd->{mr} = $pw - $tr;
            $pd->{tw} = $tr - $tl;
            $pd->{th} = $tb - $tt;

            my ($pnummargin) = POSIX::floor($ph - pdf2pt($page->{lowest_number}));
            $nummargin = min($nummargin, $pnummargin) if $pnummargin < $ph - $tb;
        }

        $px->{$i} = $pd;
        my($k, $v);
        while (($k, $v) = each %$pd) {
            $dx->{$k}->{$v} += 1;
        }
    }
    my($pw, $ph) = (modevalkey($dx->{pw}), modevalkey($dx->{ph}));
    my($tw, $th) = (modevalkey($dx->{tw}), modevalkey($dx->{th}));
    my($mt, $ml) = (modevalkey($dx->{mt}), modevalkey($dx->{ml}));
    my($mr, $mb) = (modevalkey($dx->{mr}), modevalkey($dx->{mb}));

    my ($doc_ps) = sprintf "\"papersize\": [%.0f,%.0f]", $ph, $pw;
    my ($doc_margin);
    if (defined($mt) && defined($mr)) {
        $doc_margin = sprintf "\"margin\": [%.0f,%.0f,%.0f,%.0f]", $mt, $mr, $mb, $ml;
    } else {
        $doc_margin = "\"margin\": [0,0,0,0]";
    }
    my ($doc_bfs, $doc_l);
    if (p2h_font_bug($doc)) {
        $doc_bfs = "\"bodyfontsize\": null";
        $doc_l = "\"leading\": null";
    } else {
        $doc_bfs = sprintf "\"bodyfontsize\": %g", pdf2pt($doc->{bodyfontsize});
        $doc_l = sprintf "\"leading\": %g", $doc->{lead_pt};
    }
    my ($doc_c) = sprintf "\"columns\": %d", $doc->{ncols};
    print "  $doc_ps,\n  $doc_margin,\n  $doc_bfs,\n  $doc_l,\n  $doc_c,\n";
    printf "  \"nummargin\": %.0f,\n", $nummargin if $nummargin < 10000;
    if (exists $banal_param{max_pages} && $doc->{npages} > $banal_param{max_pages}) {
        printf "  \"npages\": %d,\n", $doc->{npages};
    }
    printf "  \"w\": %d,\n", $nwords;
    print "  \"pages\": [";
    my ($sep) = "\n";

    my %pages;
    for my $i (1 .. $doc->{npages}) {
        last if exists $banal_param{max_pages} && $i > $banal_param{max_pages};
        my $page = $doc->{pages}->{$i};
        my @val = ();

        if ($page->{num} =~ /\A\d+\z/ && $page->{num} ne $i) {
            push @val, sprintf "\"pageno\": %d", $page->{num};
        } elsif ($page->{num} ne $i || $json_pagenum) {
            push @val, sprintf "\"pageno\": %s", json_quote($page->{num});
        }

        my($pd) = $px->{$i};
        my($page_ps) = sprintf "\"papersize\": [%.0f,%.0f]", $pd->{ph}, $pd->{pw};
        push @val, $page_ps if $page_ps ne $doc_ps;

        push @val, sprintf "\"type\": %s", json_quote($page->{type})
            if $page->{type} ne "body";

        if ($page->{type} ne "blank") {
            my($page_margin) = sprintf "\"margin\": [%.0f,%.0f,%.0f,%.0f]", $pd->{mt}, $pd->{pw} - ($pd->{ml} + $pd->{tw}), $pd->{ph} - ($pd->{mt} + $pd->{th}), $pd->{ml};
            push @val, $page_margin if $page_margin ne $doc_margin;
            if (exists($page->{bodyfontsize})) {
                my($page_bfs) = sprintf "\"bodyfontsize\": %g", pdf2pt($page->{bodyfontsize});
                push @val, $page_bfs if $page_bfs ne $doc_bfs;
            }
            if (exists($page->{reffontsize})
                && $page->{reffontsize} != $doc->{bodyfontsize}) {
                push @val, sprintf "\"reffontsize\": %g", pdf2pt($page->{reffontsize});
            }
            if (exists($page->{lead_pt}) && $page->{lead_pt}) {
                my($page_l) = sprintf "\"leading\": %g", $page->{lead_pt};
                push @val, $page_l if $page_l ne $doc_l;
            }
            my($page_c) = sprintf "\"columns\": %d", $page->{ncols};
            push @val, $page_c if $page_c ne $doc_c &&
                $page->{type} ne "figure" &&
                $page->{type} ne "appendix";
            push @val, sprintf "\"c\": %d", $page->{nchars};
            if (exists($page->{nwords}) && $page->{nwords} > 0) {
                push @val, sprintf "\"w\": %d", $page->{nwords};
            }
            if (exists $page->{headings} && $json_pagenum) {
                my (@ht);
                foreach my $heading (@{$page->{headings}}) {
                    push @ht, json_quote($heading);
                }
                push @val, "\"headings\": [" . join(",", @ht) . "]";
            }
            if (exists $page->{first_section}) {
                push @val, "\"fs\": " . $page->{first_section};
            }
        }

        print $sep, "    {", join(", ", @val), "}";
        $sep = ",\n";
    }
    print "\n  ]\n}\n";
}

sub report_verbose ($) {
    my ($doc) = @_;
    my ($page) = $doc->{pages}->{1};

    print $doc->{fullpath}, "\n";
    if (p2h_font_bug ($doc)) {
        print STDERR $doc->{fullpath}, "\n";
        print STDERR "$banal_filename: Error: $pdftohtml encountered font problems...some info likely bogus.\n";
    }
    printf "Paper size: %.2fin x %.2fin\n",
        pdf2in($doc->{pagebb}->{width}),
        pdf2in($doc->{pagebb}->{height});
    printf "Text region: %.2fin x %.2fin\n",
        pdf2in($doc->{textbb}->{width}),
        pdf2in($doc->{textbb}->{height});
    printf "Margins: %.2fin x %.2fin x %.2fin x %.2fin (l/r/t/b)\n",
        pdf2in($doc->{textbb}->{left}),
        pdf2in($doc->{textbb}->{rmarg}),
        pdf2in($doc->{textbb}->{top}),
        pdf2in($doc->{textbb}->{bmarg});
    printf "Body font size: %.2fpt", pdf2pt($doc->{bodyfontsize});
    if (p2h_font_bug ($doc)) {
        print " (bogus)";
    }
    print "\n";
    printf "Leading: %.1fpt\n", $doc->{lead_pt};
    print "Columns: ", $page->{ncols}, "\n";
    print "Pages: ", $doc->{npages}, "\n";

    print "\n";
    foreach my $i (1..$doc->{npages}) {
        $page = $doc->{pages}->{$i};

        print "Page $page->{num}:\n";
        printf "  text region: %.2fin x %.2fin\n",
            pdf2in($page->{textbb}->{width}),
            pdf2in($page->{textbb}->{height});

        my ($left_i) = $page->{textbb}->{left};
        my ($right_i) = $page->{pagebb}->{width} -
            ($left_i + $page->{textbb}->{width});
        my ($top_i) = $page->{textbb}->{top};
        my ($bot_i) = $page->{pagebb}->{height} -
            ($top_i + $page->{textbb}->{height});
        printf "  margins: %.2fin x %.2fin x %.2fin x %.2fin (l/r/t/b)\n",
            pdf2in($left_i), pdf2in($right_i),
            pdf2in($top_i), pdf2in($bot_i);

        if (defined($page->{bodyfontsize})) {
            printf "  body font: %gpt\n", pdf2pt($page->{bodyfontsize});
        }
        printf "  leading: %gpt\n", $page->{lead_pt};
        printf "  columns: %d\n", $page->{ncols};
        print  "  type: ", $page->{type}, "\n";

        my ($nchars) = $page->{nchars};
        printf "  nchars: %d\n", $nchars;
    }
}

sub report_stats ($) {
    my ($doc) = @_;
    my ($page) = $doc->{pages}->{1};

    if (p2h_serious_font_bug ($doc)) {
        print STDERR "$banal_filename: Error: $pdftohtml encountered font problems...skipping.\n";
        return;
    }

    if (p2h_font_bug ($doc)) {
        print STDERR "$banal_filename: Warning: $pdftohtml encountered font problems...some info likely bogus.\n";
    }

    printf  "%s\t%.2fx%.2f\t%.2fx%.2f\t%.2fx%.2fx%.2fx%.2f\t%d\t%.1f\t%d\t%d\t%s\n",
            $doc->{fullpath},
            # page width x height
            pdf2in($doc->{pagebb}->{width}),
            pdf2in($doc->{pagebb}->{height}),
            # text region width x height
            pdf2in($doc->{textbb}->{width}),
            pdf2in($doc->{textbb}->{height}),
            # margins left x right x top x bottom
            pdf2in($doc->{textbb}->{left}),
            pdf2in($doc->{textbb}->{rmarg}),
            pdf2in($doc->{textbb}->{top}),
            pdf2in($doc->{textbb}->{bmarg}),
            # body font
            pdf2pt($doc->{bodyfontsize}),
            # leading
            $doc->{lead_pt},
            # columns
            $doc->{ncols},
            # pages
            $doc->{npages},
            # app
            "-";
}

sub parse_p2h_fonts ($$) {
    my ($line, $page) = @_;
    my (%fonts, $font, $fontid, $skip, $r, $g, $b, $minc, $maxc);

    while (1) {
#       print "p2h_font: $line";
        return $line if ($line =~ /<\/page>/);

        last unless ($line =~ /<fontspec id=\"(\d+)\" size=\"(-?\d+(?:\.\d*)?)\" family=\"([^\"]+)\" color=\"([^\"]+)\"(?: opacity=\"([^\"]+)\"|)/);
        my ($idnum, $xsize, $family, $color, $opacity) =
            ($1, $2, $3, $4, defined($5) && $5 ne "" ? +$5 : 1.0);

        if ($color eq "#000000" && $opacity >= 1) {
            $skip = 0;
        } elsif ($color =~ /\Z\#[0-9a-fA-F]{6}\z/) {
            $r = hex(substr($color, 1, 2));
            $g = hex(substr($color, 2, 2));
            $b = hex(substr($color, 5, 2));
            if ($opacity >= 0 && $opacity < 1) {
                $r += (255 - $r) * (1 - $opacity);
                $g += (255 - $g) * (1 - $opacity);
                $b += (255 - $b) * (1 - $opacity);
            }
            $minc = min $r, $g, $b;
            $maxc = max $r, $g, $b;
            $skip = ($minc + $maxc) / 2 >= 191;
        } else {
            $skip = $opacity <= 0.25;
        }

        $font = {
            id => $idnum, size => rpdffont(+$xsize), family => $family,
            color => $color, opacity => $opacity, skip => $skip
        };
        $fontid = "$family//$xsize//$color//$opacity";
        if (exists $fonts{$fontid}) {
            $font->{id} = $fonts{$fontid};
        } else {
            $fonts{$fontid} = $idnum;
        }
        $page->{doc}->{fonts}{$idnum} = $font;

        $line = <$FILE>;
    }

    return $line;
}

sub check_p2h_error ($) {
    my ($line) = @_;

    # check for pdftohtml error strings embedded in output
    return 1 if ($line =~ /^stroke seems to be a pattern/);

    return 0;
}

sub xmap_make ($$;$) {
    my ($h, $rowh, $snap) = @_;
    $snap = 0 if !$snap;
    my ($nrows) = POSIX::ceil($h / $rowh);
    my ($xmap) = { rowh => $rowh, nrows => $nrows, snap => $snap, rows => [] };
    while (@{$xmap->{rows}} < $nrows) {
        push @{$xmap->{rows}}, [];
    }
    $xmap;
}

sub xmap_add ($$$$$) {
    my ($xmap, $top, $left, $width, $height) = @_;
    my ($t) = max(0, POSIX::floor($top / $xmap->{rowh}));
    my ($b) = min($xmap->{nrows}, POSIX::ceil(($top + $height) / $xmap->{rowh}));
    my ($snap) = $xmap->{snap};
    my ($right) = $left + $width;

    for (; $t < $b; $t += 1) {
        my ($z) = $xmap->{rows}->[$t];
        my ($i, $j);
        for ($i = 0; $i < @$z && $z->[$i + 1] < $left - $snap; $i += 2) {
        }
        for ($j = $i; $j < @$z && $z->[$j] <= $right + $snap; $j += 2) {
        }
        if ($j > $i + 2) {
            splice(@$z, $i + 1, $j - $i - 2);
        }
        if ($j == $i && ($i == @$z || $right < $z->[$i])) {
            splice(@$z, $i, 0, $left, $right);
        } else {
            $z->[$i] = min($z->[$i], $left);
            $z->[$i + 1] = max($z->[$i + 1], $right);
        }
    }
}

sub xmap_close_gaps ($) {
    my ($xmap) = @_;
    my ($nxmap) = xmap_make($xmap->{nrows} * $xmap->{rowh}, $xmap->{rowh}, $xmap->{snap});

    my ($t) = 0;
    foreach my $z (@{$nxmap->{rows}}) {
        push @$z, @{$xmap->{rows}->[$t]};
        my ($pz) = $t == 0 ? [] : $xmap->{rows}->[$t - 1];
        my ($pi) = 0;
        my ($nz) = $t == @{$xmap->{rows}} - 1 ? [] : $xmap->{rows}->[$t + 1];
        my ($ni) = 0;

        for (my $i = 0; $i < @$z - 2; ) {
            while ($pi < @$pz && $pz->[$pi + 1] < $z->[$i + 2]) {
                $pi += 2;
            }
            while ($ni < @$nz && $nz->[$ni + 1] < $z->[$i + 2]) {
                $ni += 2;
            }
            if (($pi < @$pz || $ni < @$nz)
                && ($pi == @$pz || $pz->[$pi] < $z->[$i + 1])
                && ($ni == @$nz || $nz->[$ni] < $z->[$i + 1])) {
                splice(@$z, $i + 1, 2);
            } else {
                $i += 2;
            }
        }

        $t += 1;
    }
    $nxmap;
}

sub xmap_strip_overlap ($$$) {
    my ($xmap, $left, $right) = @_;
    my ($nxmap) = xmap_make($xmap->{nrows} * $xmap->{rowh}, $xmap->{rowh}, $xmap->{snap});

    my ($yi) = 0;
    foreach my $z (@{$xmap->{rows}}) {
        my $i = 0;
        for (; $i < @$z && $z->[$i + 1] < $left; $i += 2) {
            push @{$nxmap->{rows}->[$yi]}, $z->[$i], $z->[$i + 1];
        }
        for (; $i < @$z && $z->[$i] < $right; $i += 2) {
        }
        for (; $i < @$z; $i += 2) {
            push @{$nxmap->{rows}->[$yi]}, $z->[$i], $z->[$i + 1];
        }
        $yi += 1;
    }
    $nxmap;
}

sub xmap_count_nonempty ($) {
    my ($xmap) = @_;
    my ($n) = 0;

    foreach my $z (@{$xmap->{rows}}) {
        $n += 1 if @$z > 0;
    }
    $n;
}

sub xmap_find_bounds ($;$) {
    my ($xmap, $frac) = @_;

    my (@lows, @highs);
    foreach my $z (@{$xmap->{rows}}) {
        if (@$z) {
            push @lows, $z->[0];
            push @highs, $z->[@$z - 1];
        }
    }

    @lows = sort { $a <=> $b } @lows;
    @highs = sort { $a <=> $b } @highs;

    $frac = 0 if !$frac;
    $frac = POSIX::floor($frac * @lows) if $frac;

    if (!@lows || $frac >= @lows) {
        (0, 0);
    } elsif (!$frac || $frac < 0) {
        ($lows[0], $highs[@highs - 1]);
    } else {
        ($lows[$frac], $highs[@highs - 1 - $frac]);
    }
}

sub xmap_print ($) {
    my ($xmap) = @_;

    for (my $y = 0; $y < @{$xmap->{rows}}; $y += 1) {
        print STDERR $y, " [", join(", ", @{$xmap->{rows}->[$y]}), "]\n";
    }
}

sub unxml ($) {
    my ($t) = @_;
    $t =~ s{<.*?>}{}g;
    $t =~ s{&.*?;}{
        if ($& eq "&gt;") {
            ">";
        } elsif ($& eq "&lt;") {
            "<";
        } elsif ($& eq "&amp;") {
            "&";
        } elsif ($& eq "&#34;") {
            "\"";
        } else {
            print STDERR "bad xml $&\n";
            $&;
        }
    }ge;
    $t;
}

sub account_text_sizes ($\%\%) {
    my($text, $alens, $tlens) = @_;
    my($content) = $text->{content};
    my($size, $len) = ($text->{sz}, length($content) + 1);
    $alens->{$size} = 0 if !exists $alens->{$size};
    $alens->{$size} += $len;
    if (!$text->{num}) {
        $tlens->{$size} = 0 if !exists $tlens->{$size};
        $tlens->{$size} += $len;
    }
}

sub bodyfontsize_reffontsize (\@$$) {
    my($texts, $refhead, $unrefhead) = @_;
    my($rl, $rr, $rt, $ul, $ur, $ut);
    if ($refhead) {
        $rl = $refhead->{coll};
        $rr = $refhead->{colr};
        $rt = $refhead->{t};
    }
    if ($unrefhead) {
        $ul = $unrefhead->{coll};
        $ur = $unrefhead->{colr};
        $ut = $unrefhead->{t};
    }
    my (%ref_alens, %unref_alens, %ref_tlens, %unref_tlens);
    foreach my $text (@$texts)  {
        my($l, $r, $t, $b) = ($text->{l}, $text->{l} + $text->{w}, $text->{t}, $text->{t} + $text->{h});
        if (($refhead && ($l < $rl || $r <= $rl || ($l <= $rr && $b <= $rt)))
            || ($unrefhead && ($l >= $ur || ($r >= $ul && $t >= $ut)))) {
            account_text_sizes($text, %text_alens, %text_tlens);
        } else {
            account_text_sizes($text, %ref_alens, %ref_tlens);
        }
    }
    (bodyfontsize(%text_alens, %text_tlens), bodyfontsize(%ref_alens, %ref_tlens));
}

sub parse_p2h_text ($$$) {
    my ($doc, $line, $page) = @_;
    my ($pagewidth) = $page->{pagebb}->{width};
    my ($pageheight) = $page->{pagebb}->{height};
    my ($fontsizefix) = $banal_param{fontsizefix};

    my (@texts, %alens, %tlens);
    my ($lowest_number) = 0;

    while (1) {
#       next if (check_p2h_error ($line));

        unless ($line =~ /<text top=\"(-?\d+(?:\.\d*)?)\" left=\"(-?\d+(?:\.\d*)?)\" width=\"(-?\d+(?:\.\d*)?)\" height=\"(-?\d+(?:\.\d*)?)\" font=\"(-?\d+)\"/) {
            # if we didn't match a <text>, then it should be an end of page or <image>
            if ($line =~ /<image/) {
                $line = <$FILE>;
                next;
            }
            unless ($line =~ /<\/page>/) {
                if ($debug_parse) {
                    print STDERR "$banal_filename: Curious, expecting a </page> but found:\n";
                    print STDERR $line;
                }
            }
            last;
        }

        my ($top) = rpdf($1);
        my ($left) = rpdf($2);
        my ($inheight) = +$4;
        my ($bottom) = $top + rpdf($inheight);
        my ($right) = $left + rpdf($3);
        my ($fontspec) = $page->{doc}->{fonts}->{$5};
        my ($font) = $fontspec->{id};
        my ($size) = $fontspec->{size};

        # embedded newlines will split <text>...</text> across multiple lines
        my ($continuation);
        while ($line !~ /<\/text>/ && defined($continuation = <$FILE>)) {
            $line .= $continuation;
        }

        # sanity check the data somewhat...text from embedded figures
        # can produce surprising values
        $left = 0 if $left < 0;
        $right = $pagewidth if $right > $pagewidth;
        $top = 0 if $top < 0;
        $bottom = $pageheight if $bottom > $pageheight;
        if ($top >= $bottom || $left >= $right) {
            $line = <$FILE>;
            next;
        }

        # skip watermarks and other light text
        if ($fontspec->{skip}) {
            $line = <$FILE>;
            next;
        }

        my ($content) = "";
        if ($line =~ /<text.*?>(.*)<\/text>/s) {
            my ($core_content) = $1;
            $content = unxml($core_content);
            if ($fontsizefix && $core_content =~ /<span/) {
                my ($size2) = rpdffont($inheight);
                $size = $size2 if $size < $size2 && $size * 1.33 >= $size2;
            }
        }
        my ($isnum) = length($content) <= 5 && $content =~ /\A[- ,.:\/0-9]*\z/;
        my ($text) = {
            t => $top, l => $left, w => $right - $left, h => $bottom - $top,
            f => $font, sz => $size, content => $content, num => $isnum
        };

        push @texts, $text;
        account_text_sizes($text, %alens, %tlens);
        if ($text->{num} && $content =~ /\d/) {
            $lowest_number = max $lowest_number, $bottom;
        }
        # print STDERR $page->{num}, " ", int(pdf2pt($left)), " ", $top, " ", $content, "\n";

        $line = <$FILE>;
    }

    # compute page body font: the font with the most characters
    my ($bodyfontsize) = bodyfontsize(%alens, %tlens);
    my ($numfontsize) = $bodyfontsize;
    if (exists($doc->{bodyfontsize}) && $doc->{bodyfontsize} > $numfontsize) {
        $numfontsize = $doc->{bodyfontsize};
    }

    # compute extent map, using extents larger than the body font
    # plus other extents that aren’t short numbers
    my ($xmap) = xmap_make($pageheight, in2pdf(1/4), in2pdf(1/16));
    my ($ymap) = xmap_make($pagewidth, in2pdf(1/4), in2pdf(1/16));
    foreach my $text (@texts) {
        if (!$text->{num} || $text->{sz} > $numfontsize) {
            xmap_add($xmap, $text->{t}, $text->{l}, $text->{w}, $text->{h});
            xmap_add($ymap, $text->{l}, $text->{t}, $text->{h}, $text->{w});
        }
    }

    # compute columns using extent map
    calc_page_columns($page, $xmap);

    # compute text region using extent maps
    calc_page_text_region($page, $xmap, $ymap);
    $page->{lowest_number} = $lowest_number;

    # compute headings
    calc_page_headings($doc, $page, \@texts, $bodyfontsize);

    # recompute font size if headings contained references
    my ($reffontsize);
    if (exists($page->{heading_texts})) {
        my ($was_inref, $inref, $refhead, $unrefhead);
        $was_inref = $inref = $doc->{in_references};
        foreach my $text (@{$page->{heading_texts}}) {
            if ($text->{is_references}) {
                $inref = 1;
                $refhead = $text;
            } elsif ($inref) {
                $inref = 0;
                $unrefhead = $text;
            }
        }
        $doc->{in_references} = $inref;

        $was_inref = 1 if $refhead && exists $refhead->{heading1};
        $was_inref = 0 if $unrefhead && exists $unrefhead->{heading1};

        if (($refhead && !$was_inref) || ($unrefhead && $was_inref)) {
            ($bodyfontsize, $reffontsize) = bodyfontsize_reffontsize(@texts, $refhead, $unrefhead);
        } elsif ($was_inref) {
            $reffontsize = $bodyfontsize;
            $bodyfontsize = undef;
        }
    } elsif ($doc->{in_references}) {
        $reffontsize = $bodyfontsize;
        $bodyfontsize = undef;
    }
    $page->{bodyfontsize} = $bodyfontsize if $bodyfontsize;
    $page->{reffontsize} = $reffontsize if $reffontsize;
    my($fontsize) = $bodyfontsize ? $bodyfontsize : $reffontsize;

    # compute leading using bodyfontsize and column specs
    calc_page_leading($page, @texts, $fontsize);

    # compute page character count using content lengths
    calc_page_nchars($page, %alens, $fontsize);

    # compute page word count using body font size only
    if ($bodyfontsize) {
        calc_page_nwords($page, @texts, $bodyfontsize);
    }
}

sub parse_p2h_page ($) {
    my ($doc) = @_;

    # assume we've just read the header
    my ($line);
    $line = <$FILE>;
    return '' if !defined($line);

    # skip any error strings embedded between pages
    while (check_p2h_error ($line)) {
        print STDERR "$banal_filename: skipping p2h error string: $line" if ($debug_parse);
        $line = <$FILE>;
    }

    if ($line !~ /<page/ && $line =~ /<outline>/) {
        my($nout) = 0;
        while (1) {
            ++$nout if $line =~ /<outline>/;
            --$nout if $line =~ /<\/outline>/;
            last if $nout == 0;
            $line = <$FILE>;
        }
        $line = <$FILE> if $line =~ /<\/outline>\s*$/;
    }

    unless ($line =~ /<page number=\"(\d+)\" position=\"([A-Za-z0-9]+\") top=\"(\d+)\" left=\"(\d+)\" height=\"(\d+)\" width=\"(\d+)\"/) {
        return '' if ($line =~ /<\/pdf2xml/);
        print STDERR "$banal_filename: Error: \"<page ...\" node expected for page ", $doc->{npages} + 1, "\n";
        chomp $line;
        print STDERR "-> '$line'\n";
        return '';
    }

    # initialize page data structures
    my ($pagebb) = {
        top => rpdf($3),
        left => rpdf($4),
        height => rpdf($5),
        width => rpdf($6),
    };

    my ($page) = {
        doc => $doc,
        num => $1,
        pagebb => $pagebb,
        max_heading_fontsize => 0,
    };

    # check for optional fontspecs at start of page
    $line = <$FILE>;
    if ($line =~ /<fontspec/) {
        $line = parse_p2h_fonts ($line, $page);
    } elsif ($debug_parse) {
        print STDERR "$banal_filename: Curious, no fontspec on page, found:\n";
        print STDERR "$line";
    }


    # process text segments
    if ($line =~ /<(?:text|image)/) {
        parse_p2h_text ($doc, $line, $page);
    } elsif ($debug_parse) {
        print STDERR "$banal_filename: Curious, empty page $page->{num}, found:\n";
        print STDERR "$line";
    }

    return $page;
}

sub parse_p2h_header ($) {
    my ($doc) = @_;

    while (<$FILE>) {
        return 1 if (/<pdf2xml/);
    }
    return 0;
}

sub merge_page ($$) {
    my ($doc, $page) = @_;

    $doc->{npages}++;
    $doc->{pages}->{$page->{num}} = $page;

    # initialize doc spec with first page spec
    if ($page->{num} == 1) {
        $doc->{pagebb} = $page->{pagebb};
        $doc->{ncols} = $page->{ncols};
    }

    # update bodyfont, leading
    if (exists $page->{bodyfontsize}) {
        my($bfs) = $page->{bodyfontsize};
        $doc->{bodyfont_counts}->{$bfs} += 1;
        if (!exists($doc->{bodyfontsize})
            || $doc->{bodyfontsize} != $bfs) {
            $doc->{bodyfontsize} = modevalkey($doc->{bodyfont_counts});
        }
        if (!exists($doc->{max_bodyfontsize})
            || $doc->{max_bodyfontsize} < $bfs) {
            $doc->{max_bodyfontsize} = $bfs;
        }
    }
    if (exists $page->{lead_pt}) {
        my($l) = $page->{lead_pt};
        $doc->{lead_counts}->{$l} += 1;
        if (!exists($doc->{lead_pt})
            || $doc->{lead_pt} != $l) {
            $doc->{lead_pt} = modevalkey($doc->{lead_counts});
        }
    }
}

sub banal_file ($) {
    my ($file) = @_;

    # initialize doc data structure
    my ($doc) = {
        width => 0,
        height => 0,
        npages => 0,
        ncols => 0,
        fonts => {},
        pages => {},
        headings => {},
        max_numeric_heading => 0,
        max_section => 0,
        app => '',
        fullpath => $file,
        filename => basename($file),
        in_references => 0,
        bodyfont_counts => {},
        leading_counts => {}
    };

    $banal_fullpath = $doc->{fullpath};
    $banal_filename = $doc->{filename};

    if (!parse_p2h_header ($doc)) {
        print STDERR "$banal_filename: Error: No $pdftohtml output...corrupted pdf file?\n";
        return;
    }

    my ($page);
    while ($page = parse_p2h_page ($doc)) {
        merge_page ($doc, $page);
    }

    calc_doc_text_region ($doc);
    calc_doc_page_types ($doc);
    calc_doc_columns ($doc);

    if ($banal_mode eq "stats") {
        report_stats ($doc);
    } elsif ($banal_mode eq "json") {
        report_json ($doc);
    } else {
        report_verbose ($doc);
    }
}

sub banal_fail ($) {
    if ($banal_mode eq "json") {
        printf "{\n";
        printf "  \"at\": %d,\n", time if !$no_timestamp;
        printf "  \"args\": %s,\n", json_quote(join(" ", @switches)) if @switches;
        printf "  \"error\": true,\n";
        printf "  \"pages\": []\n";
        printf "}\n";
    }
}

sub shell_quote ($) {
    my($s) = @_;
    $s =~ s/\'/\'\"\'\"\'/g;
    return "'$s'";
}

sub banal_open_input ($) {
    my ($fname) = @_;
    my ($base, $ext, $cmd, $oname);

    if ($fname =~ /(.+)\.(.+)/) {
        ($base, $ext) = ($1, $2);
    } else {
        print STDERR "$fname: Error: Unable to determine file type from extension.\n";
        return 0;
    }

    # 2>&1
    if ($ext =~ /^pdf$/i) {
        ($FILE, $oname) = File::Temp::tempfile("banalXXXXX", UNLINK => 1, SUFFIX => ".xml", TMPDIR => 1);
        $cmd = shell_quote($pdftohtml) . " -enc UTF-8 -xml -i"
            . " -zoom " . $banal_param{zoom}
            . ($banal_param{noroundcoord} ? " -noroundcoord" : "")
            . " " . shell_quote($fname) . " " . shell_quote($oname) . " 2>&1";
        print STDERR "$cmd\n" if $debug_pdftohtml;

        my($ignore_output) = `$cmd`;
        # Some bad `pdftohtml`s add an extra `.xml` suffix.
        if (!-s $FILE && -s "$oname.xml") {
            my $xfh = undef;
            if (!open($xfh, "<:raw :bytes", "$oname.xml")) {
                print STDERR "$oname.xml: Error: Failed to open file.\n";
                return 0;
            }
            my($str);
            while (read($xfh, $str, 16384)) {
                print $FILE $str;
            }
            close($xfh);
            unlink("$oname.xml");
            seek($FILE, 0, 0);
        }
        if (!-s $FILE) {
            print STDERR "$fname: Error: Failed to open file.\n";
            return 0;
        }
    } elsif ($ext =~ /^xml$/i) {
        unless (open ($FILE, "$fname")) {
            print STDERR "$fname: Error: Failed to open file.\n";
            return 0;
        }
    } else {
        print STDERR "$fname: Error: Failed to open file.\n";
        return 0;
    }
    binmode ($FILE, ":utf8");

    return 1;
}

sub banal_config_p2h ($) {
    my ($fname) = @_;
    $fname = basename($fname);

    my ($major, $minor, $poppler);
    unless (open(P2H, "$pdftohtml -v 2>&1 |")) {
        print STDERR "$fname: Error: Failed to run $pdftohtml.\n";
        while (defined($_ = <P2H>)) {
            print STDERR;
        }
        return 0;
    }
    while (defined($_ = <P2H>)) {
        $poppler = 1 if /Poppler/;
        next unless (/pdftohtml version (\d+\.\d+)([a-z]*)/);
        $p2h_version = "$1$2";
        $major = $1;
        $minor = $2;
    }
    close (P2H);

    if (!exists $banal_param{zoom}) {
        if (($major == 0.40) && $minor && (($minor cmp "c") >= 0)) {
            # configure for versions 0.40c and above
            $banal_param{zoom} = 10;
        } else {
            $banal_param{zoom} = 3;
        }
    }

    if (!exists $banal_param{noroundcoord}) {
        $banal_param{noroundcoord} = $major >= 0.68;
    }

    # use a default policy according to the zoom level we can use
    # at low zoom, interpolate
    if ($banal_param{zoom} >= 10 || $banal_param{noroundcoord}) {
        $banal_leading_policy = 'mode';
    } else {
        $banal_leading_policy = 'interpolate';
    }

    print "leading policy: $banal_leading_policy\n" if ($debug_leading);

    if (!exists $banal_param{fontsizefix}) {
        $banal_param{fontsizefix} = $major == 0.40;
    }

    $p2h_font_size_compensation = $major >= 0.85 ? 1 : 3;
    $p2h_scale = 10 / $banal_param{zoom};

    return 1;
}

sub banal_version () {
    print "Banal version $banal_version.\n";
    return 0;
}

sub main () {
    return banal_version () if $banal_mode eq "version";

    usage if ($#ARGV < 0);

    if (!banal_config_p2h ($ARGV[0])) {
        return 1;
    }

    my $any = 0;
    foreach my $file (@ARGV) {
        # open input file into FILE
        if (banal_open_input($file)) {
            banal_file($file);
            close $FILE;
            $any = 1;
        } else {
            banal_fail($file);
        }
    }
    return 1 - $any;
}

exit (main ());
