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
require_once $GEN_DIR.'/Thrucene.php';
require_once $GEN_DIR.'/Thrucene_types.php';
require_once $GEN_DIR.'/Thrudoc.php';
require_once $GEN_DIR.'/Bookmark_types.php';
error_reporting(E_ALL);


define("THRUCENE_PORT", 11299 );
define("THRUDOC_PORT",  11291 );

define("THRUCENE_DOMAIN", "tutorial");
define("THRUCENE_INDEX",  "bookmarks");

class BookmarkManager {
    private $thrudoc;
    private $thrucene;

    private $mbuf;
    private $mbuf_protocol;

    function __construct() {
        $this->connect_to_thrudoc();
        $this->connect_to_thrucene();

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

    private function connect_to_thrucene()
    {
        //Thrudoc connection
        $socket         = new TSocket('localhost', THRUCENE_PORT);
        $transport      = new TFramedTransport($socket);
        $protocol       = new TBinaryProtocol($transport);
        $this->thrucene = new ThruceneClient($protocol);

        $transport->open();
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

        $this->thrucene->commitAll();

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
        $id    = $this->thrudoc->add($b_str);

        return $id;
    }

    private function index_bookmark( $id, $b )
    {
        $doc      = new Thrucene_DocMsg();

        $doc->docid  = $id;
        $doc->domain = THRUCENE_DOMAIN;
        $doc->index  = THRUCENE_INDEX;

        $fields = array();
        $field = new Thrucene_Field();

        //
        //title
        //
        $field->name     = "title";
        $field->value    = $b->title;
        $field->sortable = true;
        array_push( $fields, $field);


        $field = new Thrucene_Field();
        //
        //tag
        //
        $field->name  = "tags";
        $field->value = $b->tags;
        array_push( $fields, $field);

        $doc->fields = $fields;

        $this->thrucene->add( $doc );
    }

    function remove_all()
    {
        $t0 = gettimeofday(true);

        //chunks of 100
        $offset = 0;
        $limit  = 101;

        do{

            $ids  = $this->thrudoc->listIds($offset,$limit);
            $docs = array();

            if(count($ids) == 0)
                break;

            foreach($ids as $id){
                $rm = new Thrucene_RemoveMsg();
                $rm->domain = THRUCENE_DOMAIN;
                $rm->index  = THRUCENE_INDEX;
                $rm->docid  = $id;

                array_push( $docs, $rm );
            }

             $this->thrucene->removeList($docs);
             $this->thrudoc->removeList($ids);

             $offset = $offset + $limit;

             $this->thrucene->commitAll();

        }while( count($ids) >= $limit );


        $t1 = gettimeofday(true);
        print "\n*Index cleared in: ".($t1-$t0)."*\n";
    }

    function find($terms, $options)
    {
        $func = create_function('$x,$y', 'return "$x:$y";');

        print "Searching for: '$terms' ".join(",",array_map($func, array_keys($options), array_values($options)))."\n";

        $t0 = gettimeofday(true);

        $query    = new Thrucene_QueryMsg();

        $query->domain =  THRUCENE_DOMAIN;
        $query->index  =  THRUCENE_INDEX;
        $query->query  = $terms;

        //$query->limit = 10;
        //$query->offset= 10;

        if( array_key_exists( "random", $options ) )
            $query->randomize = true;

        if( array_key_exists( "sortby", $options ) )
            $query->sortby    = $options["sortby"];


        $ids = $this->thrucene->query( $query );

        print "Found ".$ids->total." bookmarks\n";

        if( count( $ids->ids ) > 0 ){
            $bm_strs = $this->thrudoc->fetchList($ids->ids);
            $bookmarks = array();

            foreach( $bm_strs as $bm_str ){
                array_push( $bookmarks, $this->deserialize_bookmark($bm_str) );
            }

            $this->print_bookmarks($bookmarks);
        }

        $t1 = gettimeofday(true);

        print "Took: ".($t1-$t0)."\n\n";
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
