#! /bin/sh

aclocal 
touch NEWS README AUTHORS ChangeLog
automake --add-missing 
autoconf  
