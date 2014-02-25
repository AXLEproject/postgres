#!/bin/bash

BSIZE="8 16 32 64 128 256 1024"

for BS in $BSIZE; do
    	CFLAGS="-fno-omit-frame-pointer -rdynamic" ./configure --prefix=/home/adria/bin/postgres-bs${BS} --enable-debug --with-blocksize=${BS} --with-wal-blocksize=64
    	make -j16
    	make install
    	make clean
done
