#! /bin/sh

#autoscan
libtoolize --copy --force
aclocal -I config
autoheader
touch NEWS README AUTHORS ChangeLog
automake --add-missing --copy
autoconf  
