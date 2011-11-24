

import Control.Monad.Par.Scheds.Direct
import Control.Monad.IO.Class

-- An example datatype.  Of course, a real strategy would work for
-- *all* datatypes, but here we can sketch this out for a specific one.
data MyDat = A (IVar MyDat) (IVar MyDat)
           | B (IVar MyDat)
           | C (IVar Int)
           | D

-- Here's an example value.  It's tedious to construct with all those IVars!
e :: Par MyDat
e = do [a1,a2,b1,b2] <- sequence $ replicate 4 new
       c <- new
       put_ c 38
       put_ b1 (C c)
       put_ b2 D
       put_ a1 (B b1)
       put_ a2 (B b2) 
       return (A a1 a2)

-- Print values.  This is very much scrap-your boilerplate stufF:
prnt :: MyDat -> Par ()
prnt x = 
  case x of 
    A a1 a2 -> do liftIO$ putStr "A (" 
                  get a1 >>= prnt
                  liftIO$ putStr ") ("
                  get a2 >>= prnt
                  liftIO$ putStr ")"
    B b     -> do liftIO$ putStr "B (" 
                  get b >>= prnt
                  liftIO$ putStr ")"
    C c     -> do liftIO$ putStr "C (" 
                  get c >>= (liftIO . putStr . show)
                  liftIO$ putStr ")"
    D       -> liftIO$ putStr "D"


-- Simple example:
t1 = runPar (e >>= prnt) -- ==  "A (B (C (38))) (B (D))"


-- Here's a very simple path representation.  (n,m) selects the m-th
-- part of the product contained in the n-th variant of the sum.
--
-- Note this would have to get more sophisticated if there are tuples
-- or other nested data types within the RHS of each sum variant
-- (i.e. this assumes alternating sums & products).
type Path = [(Int,Int)]

path :: Path
path = [(0,0), -- Select first component of A
        (1,0), -- Select first (only) component of B
        (2,0)  -- Select first (only) component of C
        ]

-- Simple example:
t2 = runPar$
     do e' <- e
        putPath e' path 39

-- putPath populates a path from the root to a leaf in the tree structure.
-- 
-- Likewise, we have a typing difficulty here.  "Int" below should be
-- replaced by some kind of existential to deal with the heterogeneous
-- types.  SYB approach should suffice, I believe.
putPath :: MyDat -> Path -> Int -> Par ()
putPath x [(n,m)] val = 
   case (n,m,x) of 
    (2,0,C iv)   -> put_ iv val
    _            -> error "putPath failed.  Found the wrong thing at the leaf."

-- Even for this simple datatype, there is a lot of boilerplate:
putPath x ((n,m):tl) val = 
   case (n,m,x) of 
    (0,0,A a1 _ ) -> do nxt <- fillNext a1 (head tl); putPath nxt tl val
    (0,1,A _  a2) -> do nxt <- fillNext a2 (head tl); putPath nxt tl val
    (1,0,B b)     -> do nxt <- fillNext b  (head tl); putPath nxt tl val
    _             -> error "putPath failed.  Bad intermediate node."

 where 
  fillNext iv (n,m) = 
   -- Peek first to avoid unnecessary allocation:
   do p <- unsafePeek iv
      case p of
        Just v  -> return v
        Nothing -> do nu <- makeNew n
                      unsafeAtomicModify iv 
                        (\x -> case x of 
                                 Nothing -> nu
                                 Just v  -> v)

  makeNew 0 = do a1 <- new
                 a2 <- new
                 return (A a1 a2)
  makeNew 1 = do b <- new
                 return (B b)
  makeNew 2 = do c <- new
                 return (C c)
  makeNew 3 = return D


putPath2 :: MyDat -> Path -> MyDat -> Par ()
putPath2 = undefined                     

-- We don't have it now, but we could easily add unsafe operations:
unsafePeek :: IVar a -> Par (Maybe a)
unsafePeek = undefined

-- Likewise this could unsafely change the contents atomically.
-- This could be implemented with CAS or with locks.
-- (And there are many different ways to formulate this.)
unsafeAtomicModify :: IVar a -> (Maybe a -> a) -> Par a
unsafeAtomicModify = undefined
