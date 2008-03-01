use ExtUtils::MakeMaker;
WriteMakefile( 'NAME' => 'Thrudex',
               'PREREQ_PM'              => {
                   'Thrift' => 0
               },
               ($] >= 5.005 ?
                (AUTHOR     => 'T Jake Luciani <jakers@3.rdrail.net>') : ()),
               );
