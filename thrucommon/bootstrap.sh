#! /bin/sh

if ! pkg-config --version >/dev/null 2>&1; then
  echo "pkg-config is required to proceed, please install and try again...exiting" >&2
  exit 1
fi

#autoscan
libtoolize --copy --force
aclocal -I config
autoheader
touch NEWS README AUTHORS ChangeLog
automake --add-missing --copy
autoconf  
