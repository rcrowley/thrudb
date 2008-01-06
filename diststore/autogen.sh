#! /bin/sh

autoscan
autoheader
aclocal 
if libtoolize --version 1 >/dev/null 2>/dev/null; then
  libtoolize --automake
elif glibtoolize --version 1 >/dev/null 2>/dev/null; then
  glibtoolize --automake
fi
touch NEWS README AUTHORS ChangeLog
autoconf  
automake --add-missing --copy
