use ExtUtils::MakeMaker;
WriteMakefile (
    'NAME' => 'Spread::Connection',
    'VERSION_FROM' => 'lib/Spread/Connection.pm',
    'PREREQ_PM' => {
        'Spread' => 0
    },
    ($] >= 5.005 ? (AUTHOR => 'R.M. <rwmcfa1@neces.com>') : ()),
);
