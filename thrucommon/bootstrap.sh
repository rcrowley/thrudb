#!/bin/sh

#for macports
if test -d /opt/local/share/aclocal; then
  aclocal -I /opt/local/share/aclocal || exit 1
else
  aclocal || exit 1
fi

autoscan || exit 1
autoheader || exit 1

if libtoolize --version 1 >/dev/null 2>/dev/null; then
  libtoolize --automake || exit 1
elif glibtoolize --version 1 >/dev/null 2>/dev/null; then
  glibtoolize --automake || exit 1
fi

autoconf
automake -ac --add-missing --foreign || exit 1
