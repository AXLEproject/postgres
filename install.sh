#!/bin/bash -e

BASEDIR=$(dirname "$0")
BASEDIR=$(cd "$BASEDIR"; pwd)

MAKETEXT='
CFLAGS+=-mno-sse4 -mno-sse4.1 -mno-sse4.2 -mno-sse4a -mno-avx -I${ZSIMPATH}/misc/hooks\n
'

DEST=$BASEDIR/build/bin/postgres

if [ ! -r $DEST ] || [ $(find src -newer $DEST | wc -l) -gt 0 ]; then
        # compile
        mkdir -p $BASEDIR/build
        cd $BASEDIR/build
        CFLAGS="-fno-omit-frame-pointer -rdynamic" $BASEDIR/configure --prefix=$BASEDIR/build --enable-debug --with-blocksize=32 --with-wal-blocksize=64
        make clean
        echo -e $MAKETEXT >> $BASEDIR/build/src/Makefile.global
        make -j$(grep -c ^processor /proc/cpuinfo)
        make install
        echo "done. Cleaning intermediate make state"
        make clean
else
        echo ""
        echo "Already up to date. Skip compilation."
fi

echo "Add 'PGPATH=$BASEDIR' to your environment."

