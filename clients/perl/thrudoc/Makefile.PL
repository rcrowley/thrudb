use ExtUtils::MakeMaker;
WriteMakefile( 'NAME' => 'Thrudoc',
               'PREREQ_PM'              => {
                   'Thrift' => 0
               },
               ($] >= 5.005 ?
                (AUTHOR     => 'T Jake Luciani <jakers@3.rdrail.net>') : ()),
               );
