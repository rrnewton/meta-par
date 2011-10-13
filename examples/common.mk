
# RRN: allow setting it via the command line.  (I don't have a ghc-testing2 ;-). )
ifeq (,$(GHC))
  GHC = ghc
  # GHC = ghc-testing2
  # GHC = ghc-stable-nightly2
endif

# This is hard to keep up to date:
ALLPARSRC= ../Control/Monad/Par.hs ../Control/Monad/Par/AList.hs ../Control/Monad/Par/OpenList.hs \
           ../Control/Monad/Par/IList.hs  ../Control/Monad/Par/Stream.hs ../Control/Monad/Par/Logging.hs

default: all

defaultclean:
	rm -f $(EXES) *.o *.hi *.out
