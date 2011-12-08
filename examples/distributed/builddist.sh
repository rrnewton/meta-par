#!/bin/bash

# Comment to disable debug mode:
# OPTS="-DDEBUG"

ghc -i../.. --make parfib_dist.hs -O2 -threaded -rtsopts -fforce-recomp $OPTS $@
cp parfib_dist worker/

echo "Next run this command here and in the worker/ dir:"
echo "./parfib_dist monad 10 +RTS -N2"

