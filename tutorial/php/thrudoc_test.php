#!/usr/bin/env php
<?php

#
# Thrudoc test
# Richard Crowley <richard@opendns.com>
#
# A much simpler test that covers only Thrudoc's get and put methods
#

# Thrift
$THRIFT_ROOT = realpath(dirname(__FILE__) . '/../../../thrift/lib/php/src');
require_once "$THRIFT_ROOT/Thrift.php";
require_once "$THRIFT_ROOT/protocol/TBinaryProtocol.php";
require_once "$THRIFT_ROOT/transport/TSocket.php";
require_once "$THRIFT_ROOT/transport/TFramedTransport.php";
require_once "$THRIFT_ROOT/transport/TMemoryBuffer.php";

# Thrudoc
error_reporting(E_NONE);
$GEN_DIR = realpath(dirname(__FILE__) . '/../gen-php');
require_once "$GEN_DIR/Thrudoc.php";
require_once "$GEN_DIR/Thrudoc_types.php";
error_reporting(E_ALL);

$THRUDOC_PORT = 11291;
$THRUDOC_BUCKET = 'foo';

# Connect
try {
	$socket = new TSocket('localhost', $THRUDOC_PORT);
	$transport = new TFramedTransport($socket);
	$protocol = new TBinaryProtocol($transport);
	$thrudoc = new ThrudocClient($protocol);
	$transport->open();
} catch (Exception $e) {
	echo "connect failed:\n$e\n";
}

# Get (and fail)
try { echo $thrudoc->get($THRUDOC_BUCKET, 'foo'), "\n"; }
catch (Thrudoc_ThrudocException $e) { echo "Get 1 failed (expected):\n$e\n"; }

# Put
try { echo $thrudoc->put($THRUDOC_BUCKET, 'foo', 'bar'), "\n"; }
catch (Thrudoc_ThrudocException $e) { echo "Put failed:\n$e\n"; }

# Get (and succeed)
try { echo $thrudoc->get($THRUDOC_BUCKET, 'foo'), "\n"; }
catch (Thrudoc_ThrudocException $e) { echo "Get 2 failed:\n$e\n"; }

# Append
try { echo $thrudoc->append($THRUDOC_BUCKET, 'foo', ' baz'), "\n"; }
catch (Thrudoc_ThrudocException $e) { echo "Append failed:\n$e\n"; }

# Get (and succeed)
try { echo $thrudoc->get($THRUDOC_BUCKET, 'foo'), "\n"; }
catch (Thrudoc_ThrudocException $e) { echo "Get 3 failed:\n$e\n"; }

# Remove
try { echo $thrudoc->remove($THRUDOC_BUCKET, 'foo'), "\n"; }
catch (Thrudoc_ThrudocException $e) { echo "Remove failed:\n$e\n"; }
