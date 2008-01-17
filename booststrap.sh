#!/bin/sh

for i in thrucommon thrudex thrudoc thruqueue; do
    echo booststrap.sh $i;
    (cd $i && ./bootstrap.sh);
done
