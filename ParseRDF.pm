#==========================================================
# $Id: ParseRDF.pm,v 0.13 2004/01/09 18:07:42 paul Exp $
#==========================================================
# Copyright Paul Wilson 2003. All Rights Reserved.
# You may distribute this module under the terms of either 
# the GNU General Public License or the Artistic License, 
# as specified in the Perl README file.
#==========================================================
    package DMOZ::ParseRDF;
#==========================================================

    use Carp;
    use strict;
    use vars qw/$ERRORS $VERSION $ATTRIBS $error/;

    $error   = '';
    $VERSION = sprintf("%d.%02d", q$Revision: 0.13 $ =~ /(\d+)\.(\d+)/o);
    $ATTRIBS = {
        rdf_gzip_stream => 0,
        rdf_local_file  => undef,
        rdf_gzip_path   => undef,
        rdf_part_stack  => {}
    };
    $ERRORS  = {
        RDF_BAD_ARGS   => "Invalid argument(s) '%s' passed to '%s' method.",
        RDF_LOCAL_PATH => "The content file '%s' is not a valid gzip or rdf file (or cannot be read).",
        RDF_BAD_GZIP   => "The file '%s' is not a valid gzip binary.",
        RDF_BAD_FILE   => "The rdf/gzip file to be parsed has not been specified.",
        RDF_OPEN_FILE  => "Unable to read content file '%s'. Reason: %s (%s)",
        RDF_GZIP_PLAIN => "Unable to decompress a plain text content file with gzip.",
        RDF_NO_PARTS   => "There are no category parts defined. No parsing necessary.",
        RDF_READ_FILE  => "Unable to read content file '%s'. Reason: %s",
        RDF_WRITE_CAT  => "Unable to write part file '%s'. Reason: %s",
        RDF_BAD_PART   => "The category part name '%s' does not seem valid.",
        RDF_BAD_DEST   => "The category part destination file '%s' is not in a writable directory."
    };

#==========================================================

sub new {
#----------------------------------------------------------
# Create a new object.

    my ($class, $opts) = @_;
    my $self = bless {}, $class;
    if ($opts) {
        ref($opts) eq 'HASH' or croak(sprintf($ERRORS->{RDF_BAD_ARGS}, $opts, 'new'));
        foreach my $attr (keys %$ATTRIBS) {
            $self->{$attr} = 
                exists($opts->{$attr})    ? $opts->{$attr} : 
                exists($opts->{"-$attr"}) ? $opts->{$attr} : $ATTRIBS->{$attr};
        }
    }
    return $self;
}

sub data {
#----------------------------------------------------------
# Specify the path of the RDF file - can be an RDF or GZ

    my ($self, $path) = @_;
    if ($path) {
        return _set_error(sprintf($ERRORS->{RDF_LOCAL_PATH}, $path)) unless (-e $path && -r _ && (-B _ || -T _));
        $self->{rdf_local_file} = $path;
    }
    return $self->{rdf_local_file};
}

sub gzip_stream {
#----------------------------------------------------------
# We try to auto-detect anyway but this overrides.

    my ($self, $gz) = @_;
    if (defined($gz)) {
        $self->{rdf_gzip_stream} = $gz ? 1 : 0; # Any true value will do.
    }
    return $self->{rdf_gzip_stream};
}

sub gzip {
#----------------------------------------------------------
# Set the path to the gzip binary.

    my ($self, $gz) = @_;
    if ($gz) {
        -x $gz or return _set_error(sprintf($ERRORS->{RDF_BAD_GZIP}, $gz));
        $self->{rdf_gzip_path} = $gz;
    }
    return $self->{rdf_gzip_path};
}

sub parts {
#----------------------------------------------------------
# Set the categories to parse.

    my ($self, @args) = @_;
    if (@args) {
        if    (ref($args[0]) eq 'HASH') { $self->{rdf_part_stack} = $args[0] }
        elsif (@args && !(@args % 2))   { $self->{rdf_part_stack} = {@args}  }
        else {
            return _set_error(sprintf($ERRORS->{RDF_BAD_ARGS}, $args[0], 'parse'));
        }
        foreach my $part (keys %{$self->{rdf_part_stack}}) {
            my ($dest, $path) = $self->{rdf_part_stack}->{$part};
            unless (index($part, '/') > -1) {
                return _set_error(sprintf($ERRORS->{RDF_BAD_PART}, $part));
            }
            require File::Basename;
            $path = File::Basename::dirname($dest);
            if ($path) {
                return _set_error(sprintf($ERRORS->{RDF_BAD_DEST}, $dest)) unless (-e $path && -d _ && -r _ && -w _);
            }            
        }
    }
    return wantarray ? %{$self->{rdf_part_stack}} : $self->{rdf_part_stack};
}

sub parse {
#----------------------------------------------------------
# Parse the (de)compressed rdf file.

    my $self = shift;
    my $cats = $self->parts() or return _set_error($ERRORS->{RDF_NO_PARTS});
    my $gzip = $self->gzip() || $self->_find_gzip();
    my $open = 0;
    my $pipe = $self->{rdf_local_file};
    my $fhnd;

# If we are using gzip but no location was given - honk.
    if ($self->gzip_stream()) {
        return _set_error(sprintf($ERRORS->{RDF_BAD_GZIP}, $gzip)) unless (-x $gzip);
        $pipe = qq!$gzip -dc $self->{rdf_local_file} |!;
    }

# Sort the categories so we can parse the parts in order.
    %$cats = map { $_ => $cats->{$_} } sort { lc($a) cmp lc($b) } keys %$cats;

# Get the name of the fisrt category and begin parsing.
    my $cat = (keys(%$cats))[0]; 
    $| = 1;
    open RDF, $pipe or return _set_error(sprintf($ERRORS->{RDF_READ_FILE}, $self->{rdf_local_file}, $!));
    while (<RDF>) {
        if (/<Topic r:id="$cat">/ && ! $open) {
            $fhnd = \do { local *FH; *FH };
            $pipe = $self->gzip_stream() ? ($pipe . ' ' . $cats->{$cat}) : $cats->{$cat};
            open $fhnd, '>', $cats->{$cat} or return _set_error(sprintf($ERRORS->{RDF_WRITE_CAT}, $cats->{$cat}, $!));
            $open = 1;
        }
        elsif (/<Topic r:id="([^"]+)">/ && $open) {
            my $new = $1;
            if (substr($new, 0, length($cat)) ne $cat) {
                close $fhnd;
                delete $cats->{$cat};
                if (scalar keys %$cats) {
                    $cat = (keys(%$cats))[0];
                    $open = 0;
                    next;
                }
                last;
            }
        }
        next unless ($open);
        print $fhnd $_;
    }

    return 1;
}

sub _find_gzip {
#----------------------------------------------------------
# Try to locate gzip.

    my @poss = qw!gzip /bin/gzip /usr/local/bin/gzip /usr/bin/gzip!;
    for (@poss) { return $_ if (-x $_) }
    return undef;
}

sub error      { return $error         }
sub _set_error { $error = $_[0]; undef }

DESTROY {
#----------------------------------------------------------
# Flush the error in a persistant environment.

    $error = '';
}

1;

__END__

=head1 NAME

DMOZ::ParseRDF - Parse the I<gigantic> dmoz.org content file into smaller parts.

=head1 SYNOPSIS

   use DMOZ::ParseRDF;
   my $dmoz = DMOZ::ParseRDF->new({
       -rdf_gzip_stream => 1,
       -rdf_local_file  => 'content.rdf.u8.gz',
       -rdf_gzip_path   => '/bin/gzip',
       -rdf_part_stack  => {
           'Top/Arts' => '/home/dmoz/arts.part'
       }
   });

=head1 DESCRIPTION

DMOZ::ParseRDF is an object-oriented module for parsing DMOZ data into
manageable sub-sections. As of January 8th 2004 the DMOZ content file is
around 1.3GB in size. The data is free to download and can be used in your
custom database but please make sure you read the license agreement at
L<http://dmoz.org/license.html> first.

=head2 METHODS

=over4

=item $class->new( [HASHREF] )

The L<new> method creates and returns a blessed object reference. If
desired you may pass in a hash reference of options or alternatively
you can call the appropriate method to set a value for the corresponding
attribute.

The available attributes are listed below:

=over4 

=item rdf_gzip_steam

This attribute specifies whether the content file should be streamed
through gzip and can either be 1 for I<yes> or 0 for I<no>.

=item rdf_gzip_path

In order to stream the content file through gzip the path to the gzip
binary must be specified. If no path is given an attempt will be made
to guess the location if (possible).

=item rdf_local_file

This attribute sets the location of the content file. It can either be
the uncompressed content.rdf.u8 file or the gzipped version.

=item rdf_part_stack

This attribute must be a hash reference of categories to parse. The hash
key is the full category name and the corresponding value is the file
to write the parsed data to.

=back

=item $object->data( STRING )

This method is used to set the location of the DMOZ content file. It is
simply the full server path to the content.rdf file, either in gzip or 
rdf format.

=item $object->gzip_stream( BOOLEAN )

Call this method to specify whether to stream the file through gzip. The
argument must either be 1 for I<yes> or 0 for I<no>.

=item $object->gzip( STRING )

This method sets the location of the gzip binary. It is not essential to
call this method if your gzip binary is in a typical location as gzip is
looked for automatically, however calling this method ensures the correct
binary is used.

=item $object->parts( HASHREF )

This method requires a hash reference as an argument. The hash keys should
be the category name to parse, such as I<Top/Arts> and the hash key should
be the corresponding output file location, for example I</root/arts.part>.

The hash is sorted before parsing begins to ensure that the parser can parse
each category alphabetically (which is how the RDF file is formatted).

=item $object->parse()

Calling this method triggers the parser. It is called with no arguments.

=back

=head1 EXAMPLES

Here are some simple examples of how to use DMOZ::ParseRDF

This example sets attributes by calling methods...

    my $dmoz = DMOZ::ParseRDF->new();
    $dmoz->data('/home/dmoz/content.rdf.u8.gz');
    $dmoz->gzip_stream(1);
    $dmoz->gzip('/bin/gzip');
    $dmoz->parts({
        'Top/Health' => '/home/dmoz/health.part'
    });
    $dmoz->parse() or die $dmoz->error();

This example sets attributes at the time of creating the object...

    my $dmoz = DMOZ::ParseRDF->new({
        -rdf_local_file  => '/home/dmoz/content.rdf.u8.gz',
        -rdf_gzip_path   => '/bin/gzip',
        -rdf_gzip_stream => 1,
        -rdf_part_stack  => {
            'Top/Health' => '/home/dmoz/health.part'
        }
    });
    $dmoz->parse() or die $dmoz->error();

=head1 PERFORMANCE

Due to the nature of this module and the tasks it performs, it hogs CPU power. 
Fortunately the memory usage stays quite low as the data is not stored in 
memory.

Just be aware that CPU power will briefly hit the roof while parsing the DMOZ 
content.

=head1 SUPPORT

Please email I<E<lt>paul@wilsonprograms.comE<gt>> for support. You may also visit the
Wilson Programming support forum at L<http://wilsonprograms.com/cgi-bin/bb/gforum.cgi>

=head1 AUTHOR

DMOZ::ParseRDF was written by Paul Wilson I<E<lt>paul@wilsonprograms.comE<gt>> in 2004

=head1 COPYRIGHT

Copyright (c) Paul Wilson and Wilson Programming 2004. All Rights Reserved.

=cut

