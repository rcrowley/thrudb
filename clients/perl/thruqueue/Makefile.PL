use ExtUtils::MakeMaker;
WriteMakefile( 'NAME' => 'Thruqueue',
               'PREREQ_PM'              => {
                   'Thrift' => 0
               },
               ($] >= 5.005 ?
                (AUTHOR     => 'T Jake Luciani <jake@3.rdrail.net>') : ()),
               );
