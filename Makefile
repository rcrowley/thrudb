#
#
#

all:

# just pass whatever you type here on through to the children
%:
	for i in thrucommon thrudex thrudoc thruqueue; do	\
	    echo $$i;						\
	    (cd $$i && make "$@");				\
	done
