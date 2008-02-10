#!/usr/bin/env php
<?php

//Thrift Globals
$GLOBALS['THRIFT_ROOT'] = '/usr/lib/php5/thrift';

require_once $GLOBALS['THRIFT_ROOT'].'/Thrift.php';
require_once $GLOBALS['THRIFT_ROOT'].'/protocol/TBinaryProtocol.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TSocket.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TFramedTransport.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TMemoryBuffer.php';

error_reporting(E_NONE);
$GEN_DIR = '../gen-php';
require_once $GEN_DIR.'/Thrudex.php';
require_once $GEN_DIR.'/Thrudex_types.php';
require_once $GEN_DIR.'/Thrudoc.php';
require_once $GEN_DIR.'/Thrudoc_types.php';
require_once $GEN_DIR.'/Bookmark_types.php';
error_reporting(E_ALL);


define("THRUDEX_PORT", 11299 );
define("THRUDOC_PORT",  11291 );

define("THRUDOC_BUCKET", "bookmarks");

define("THRUDEX_INDEX",  "bookmarks");

class BookmarkManager {
    private $thrudoc;
    private $thrudex;

    private $mbuf;
    private $mbuf_protocol;

    function __construct() {
        $this->connect_to_thrudoc();
        $this->connect_to_thrudex();

        $this->mbuf          = new TMemoryBuffer();
        $this->mbuf_protocol = new TBinaryProtocol($this->mbuf);
    }

    private function connect_to_thrudoc()
    {
        //Thrudoc connection
        $socket         = new TSocket('localhost', THRUDOC_PORT);
        $transport      = new TFramedTransport($socket);
        $protocol       = new TBinaryProtocol($transport);
        $this->thrudoc  = new ThrudocClient($protocol);

        $transport->open();
    }

    private function connect_to_thrudex()
    {
        //Thrudex connection
        $socket         = new TSocket('localhost', THRUDEX_PORT);
        $transport      = new TFramedTransport($socket);
        $protocol       = new TBinaryProtocol($transport);
        $this->thrudex = new ThrudexClient($protocol);

        $transport->open();

        $this->thrudex->admin("create_index", THRUDEX_INDEX);
    }

    private function serialize_bookmark($b)
    {

        //Convert object to string
        $b->write($this->mbuf_protocol);

        $b_str = $this->mbuf->read( $this->mbuf->available() );


        return $b_str;
    }

    private function deserialize_bookmark($b_str)
    {
        $b = new Tutorial_Bookmark();

        //set the buffer
        $this->mbuf->write($b_str);

        $b->read($this->mbuf_protocol);

        return $b;
    }

    function load_tsv_file($file)
    {
        $t0 = gettimeofday(true);

        $fh = fopen($file, 'r');

        while( $line = fgets($fh) ){
            $arr = split("\t", $line);

            $b = new Tutorial_Bookmark();
            $b->url   = $arr[0];
            $b->title = $arr[1];
            $b->tags  = $arr[2];

            $this->add_bookmark($b);
        }

        fclose($fh);

        $t1 = gettimeofday(true);

        print "*Indexed file in: ".($t1-$t0)."*\n\n";
    }

    private function add_bookmark( $b )
    {
        $id = $this->store_bookmark($b);
        $this->index_bookmark($id,$b);
    }

    private function get_bookmark($id)
    {
        $b_str = $this->thrudoc->fetch($id);
        return $this->deserialize_bookmark($b_str);
    }

    private function store_bookmark( $b )
    {
        $b_str = $this->serialize_bookmark($b);
        $id    = $this->thrudoc->putValue(THRUDOC_BUCKET,$b_str);

        return $id;
    }

    private function index_bookmark( $id, $b )
    {
        $doc      = new Thrudex_Document();

        $doc->key    = $id;
        $doc->index  = THRUDEX_INDEX;

        $fields = array();
        $field = new Thrudex_Field();

        //
        //title
        //
        $field->key      = "title";
        $field->value    = $b->title;
        $field->sortable = true;
        array_push( $fields, $field);


        $field = new Thrudex_Field();
        //
        //tag
        //
        $field->key   = "tags";
        $field->value = $b->tags;
        array_push( $fields, $field);

        $doc->fields = $fields;

        $this->thrudex->put( $doc );
    }

    function remove_all()
    {
        $t0 = gettimeofday(true);

        //chunks of 100
        $limit  = 100;
        $seed   = "";
        do{

            $r    = $this->thrudoc->scan(THRUDOC_BUCKET,$seed,$limit);
            $docs = array();

            if(count($r->elements) == 0)
                break;

            foreach($r->elements as $id){
                $el = new Thrudex_Element();
                $el->index  = THRUDEX_INDEX;
                $el->key    = $id->key;

                array_push( $docs, $el );
            }

             $this->thrudex->removeList($docs);
             $this->thrudoc->removeList($r->elements);

             $seed = $r->seed;

        }while( count($r->elements) >= $limit );


        $t1 = gettimeofday(true);
        print "\n*Index cleared in: ".($t1-$t0)."*\n";
    }

    function find($terms, $options)
    {
        $func = create_function('$x,$y', 'return "$x:$y";');

        print "Searching for: '$terms' ".join(",",array_map($func, array_keys($options), array_values($options)))."\n";

        $t0 = gettimeofday(true);

        $query    = new Thrudex_SearchQuery();

        $query->index  =  THRUDEX_INDEX;
        $query->query  = $terms;

        //$query->limit = 10;
        //$query->offset= 10;

        if( array_key_exists( "random", $options ) )
            $query->randomize = true;

        if( array_key_exists( "sortby", $options ) )
            $query->sortby    = $options["sortby"];


        $els = $this->thrudex->search( $query );

        print "Found ".$els->total." bookmarks\n";

        if( count( $els->elements ) > 0 ){
            $response  = $this->thrudoc->getList( $this->create_doc_list($els->elements) );
            $bookmarks = array();

            foreach( $response as $doc ){
                if( $doc->element->value != null )
                    array_push( $bookmarks, $this->deserialize_bookmark($doc->element->value) );
                else
                    print "Could not get Document: \n";
            }

            $this->print_bookmarks($bookmarks);
        }

        $t1 = gettimeofday(true);

        print "Took: ".($t1-$t0)."\n\n";
    }

    private function create_doc_list( $ids )
    {
        $docs = array();

        foreach( $ids as $id ){
            $doc = new Thrudoc_Element();

            $doc->key    = $id->key;
            $doc->bucket = THRUDOC_BUCKET;

            array_push( $docs, $doc );
        }

        return $docs;
    }

    private function print_bookmarks( $bookmarks )
    {
        $i = 1;

        foreach($bookmarks as $b){
            print "$i .\ttitle :\t".$b->title."\n";
            print "\turl   :\t(".$b->url.")\n";
            print "\ttags  :\t(".$b->tags.")\n";
            $i++;
        }

    }
}

//################################
//#            Main
//################################

$bm = new BookmarkManager();

try{

    $bm->load_tsv_file("../bookmarks.tsv");

    $bm->find("tags:(+css +examples)",array( "random" => true));

    $bm->find("title:(linux)",array("sortby" => "title"));

    $bm->remove_all();

}catch(TException $tx) {
  print 'TException: '.$tx->getMessage()."\n";
}

?>
