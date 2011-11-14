#!/bin/bash

ghc -i../.. --make parfib_dist.hs -O2 -threaded -rtsopts -fforce-recomp # -DDEBUG
time ./parfib_dist monad $1 +RTS -N2 -RTS
