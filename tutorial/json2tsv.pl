use JSON;

$obj = jsonToObj(<>);

foreach my $o (@$obj){
    print join("\t", ($o->{u},$o->{d},join(" ",@{$o->{t}})))."\n";
}
