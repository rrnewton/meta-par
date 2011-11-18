#!/bin/bash
N=$1
shift
ghc -i../.. --make parfib_dist.hs -O2 -threaded -rtsopts -fforce-recomp $@
cp parfib_dist worker/
pushd worker
./parfib_dist monad $N +RTS -N2 -RTS & 
popd
time ./parfib_dist monad $N +RTS -N2 -RTS
kill -9 $!