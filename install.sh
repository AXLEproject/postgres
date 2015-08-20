#!/bin/bash -e

BASEDIR=$(dirname "$0")
BASEDIR=$(cd "$BASEDIR"; pwd)

DEST=$BASEDIR/build/bin/postgres

if [ ! -r $DEST ] || [ $(find src -newer $DEST | wc -l) -gt 0 ]; then
        # compile
        mkdir -p $BASEDIR/build
        cd $BASEDIR/build
        CFLAGS="-fno-omit-frame-pointer -rdynamic -O2" $BASEDIR/configure --prefix=$BASEDIR/build --enable-debug --with-blocksize=8 --with-wal-blocksize=8
        make clean
        make -j$(grep -c ^processor /proc/cpuinfo)
        make install
        echo "done. Cleaning intermediate make state"
        make clean
else
        echo ""
        echo "Already up to date. Skip compilation."
fi

echo "Add 'PGPATH=$BASEDIR' to your environment."

