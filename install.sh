#!/bin/bash -e

BASEDIR=$(dirname "$0")
BASEDIR=$(cd "$BASEDIR"; pwd)

DEST=$BASEDIR/build/base95/bin/postgres/

if [ ! -r $DEST ] || [ $(find src -newer $DEST | wc -l) -gt 0 ]; then
        # compile
        mkdir -p $BASEDIR/build/base95
        #cd $BASEDIR/build
        CFLAGS="-fno-omit-frame-pointer -rdynamic -O2" $BASEDIR/configure --prefix=$BASEDIR/build/base95 --with-blocksize=8 --with-wal-blocksize=8
        #make clean
        make -j$(grep -c ^processor /proc/cpuinfo)
        make install
        echo "done"
        #make clean
else
        echo ""
        echo "Already up to date. Skip compilation."
fi

echo "Creating symlinks"
rm -rf $BASEDIR/build/bin/
mkdir -p $BASEDIR/build/bin/
ln -s $BASEDIR/build/base95/bin/* $BASEDIR/build/bin

echo "Add 'PGPATH=$BASEDIR' to your environment."

