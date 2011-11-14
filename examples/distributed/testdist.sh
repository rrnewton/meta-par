#!/bin/bash

ghc -i../.. --make parfib_dist.hs -O2 -threaded -rtsopts -fforce-recomp #-DDEBUG
cp parfib_dist worker/
pushd worker
./parfib_dist monad $1 +RTS -N2 -RTS & 
popd
time ./parfib_dist monad $1 +RTS -N2 -RTS
kill -9 $!