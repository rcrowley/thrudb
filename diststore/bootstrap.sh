#! /bin/sh

#autoscan
aclocal -I config
autoheader
touch NEWS README AUTHORS ChangeLog
automake --add-missing --copy
autoconf  
