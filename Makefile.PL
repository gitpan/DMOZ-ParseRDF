use ExtUtils::MakeMaker;
WriteMakefile(
	NAME		=> "DMOZ::ParseRDF",
	VERSION_FROM	=> 'ParseRDF.pm',
	'dist'		=> {
			     COMPRESS	=> 'gzip -9f',
	      		     SUFFIX	=> 'gz',
	      		     DIST_DEFAULT => 'all tardist',
			   },
	MAN3PODS	=> {},
);

