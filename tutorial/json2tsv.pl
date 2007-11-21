#if youd like to test this with more or diff bookmarks heres what todo:
#
#First, grab json formatted bookmarks
#curl "http://del.icio.us/feeds/json/tjake?raw=1&count=100" > bookmarks.json
#
#The, run this to turn it to a tsv file
#json2tsv.pl < bookmarks.json > bookmarks.tsv



use JSON;

$obj = jsonToObj(<>);

foreach my $o (@$obj){
    print join("\t", ($o->{u},$o->{d},join(" ",@{$o->{t}})))."\n";
}
