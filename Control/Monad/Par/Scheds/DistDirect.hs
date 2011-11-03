{-# LANGUAGE RankNTypes, NamedFieldPuns, BangPatterns,
             ExistentialQuantification, CPP, ScopedTypeVariables,
             TypeSynonymInstances, MultiParamTypeClasses,
             GeneralizedNewtypeDeriving, PackageImports
	     #-}
{-# OPTIONS_GHC -Wall -fno-warn-name-shadowing -fno-warn-unused-do-bind #-}

-- | A scheduler for the Par monad based on directly performing IO
-- actions when Par methods are called (i.e. without using a lazy
-- trace data structure).

module Control.Monad.Par.Scheds.DistDirect (
   Sched(..), Par,
   IVar(..), IVarContents(..),
    runPar,
    runParIO,
    runParDist, 
    new, get, put_, fork,
    newFull, newFull_, put,
    spawn, spawn_, spawnP,
 ) where

import Control.Applicative
import Control.Concurrent hiding (yield)
import Data.IORef
import qualified Data.Vector as V
import Data.Vector (Vector)
import Text.Printf
import GHC.Conc
import "mtl" Control.Monad.Cont as C
import System.Random as Random
import System.IO.Unsafe (unsafePerformIO)
import System.Mem.StableName
import qualified Control.Monad.Par.Class as PC
import Control.DeepSeq

import Remote ( ProcessM )

--------------------------------------------------------------------------------
-- Configuration Toggles 
--------------------------------------------------------------------------------

-- #define DEBUG
#ifdef DEBUG
dbg = True
#else
dbg = False
#endif

#define FORKPARENT

--------------------------------------------------------------------------------
-- Core type definitions
--------------------------------------------------------------------------------

-- Our monad stack looks like this:
--      ---------
--        ContT
--         IO
--      ---------
-- Note that the result type for continuations is unit.  Forked
-- computations return nothing.
--
newtype Par a = Par { unPar :: C.ContT () IO a }
    deriving (Monad, MonadIO, MonadCont)

data Sched = Sched 
    { 
      ---- Per worker ----
      no       :: {-# UNPACK #-} !Int,
      workpool :: HotVar (Deque (Par ())),
      rng      :: HotVar StdGen -- Random number gen for work stealing.
     }
    deriving (Show)

newtype IVar a = IVar (IORef (IVarContents a))

data IVarContents a = Full a | Empty | Blocked [a -> Par ()]


--------------------------------------------------------------------------------
-- Helpers #1:  Simple Deques
--------------------------------------------------------------------------------

emptydeque :: Deque a 
addfront  :: a -> Deque a -> Deque a
addback   :: a -> Deque a -> Deque a

-- takefront :: Deque a -> Maybe (Deque a, a)
takefront :: Deque a -> (Deque a, Maybe a)
takeback  :: Deque a -> (Deque a, Maybe a)

dqlen :: Deque a -> Int

-- [2011.03.21] Presently lists are out-performing Seqs:
newtype Deque a = DQ [a]
emptydeque = DQ []

addfront x (DQ l)    = DQ (x:l)

addback x (DQ [])    = DQ [x]
addback x (DQ (h:t)) = DQ (h : rest)
 where DQ rest = addback x (DQ t)

takefront (DQ [])     = (emptydeque, Nothing)
takefront (DQ (x:xs)) = (DQ xs, Just x)

-- EXPENSIVE:
takeback  (DQ [])     = (emptydeque, Nothing)
takeback  (DQ ls)     = (DQ rest, Just final)
 where 
  (final,rest) = loop ls []
  loop [x]    acc = (x, reverse acc)
  loop (h:tl) acc = loop tl (h:acc)
 
dqlen (DQ l) = length l

--------------------------------------------------------------------------------
-- Helpers #2:  Atomic Variables
--------------------------------------------------------------------------------
-- TEMP: Experimental

newHotVar      :: a -> IO (HotVar a)
modifyHotVar   :: HotVar a -> (a -> (a,b)) -> IO b
modifyHotVar_  :: HotVar a -> (a -> a) -> IO ()
writeHotVar    :: HotVar a -> a -> IO ()
readHotVar     :: HotVar a -> IO a
-- readHotVarRaw  :: HotVar a -> m a
-- writeHotVarRaw :: HotVar a -> m a

{-# INLINE newHotVar     #-}
{-# INLINE modifyHotVar  #-}
{-# INLINE modifyHotVar_ #-}
{-# INLINE readHotVar    #-}
{-# INLINE writeHotVar   #-}


type HotVar a = IORef a
newHotVar     = newIORef
modifyHotVar  = atomicModifyIORef
modifyHotVar_ v fn = atomicModifyIORef v (\a -> (fn a, ()))
readHotVar    = readIORef
writeHotVar   = writeIORef
instance Show (IORef a) where 
  show _ = "<ioref>"

--------------------------------------------------------------------------------
-- Main Implementation
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Global Structures

{-# NOINLINE globals #-}
-- | Idle semaphore and a global vector of schedulers for nested
-- scheduling. Should only be initialized once per execution. Note
-- that access to the vector itself does not need to be synchronized,
-- so this is an ordinary IORef rather than a HotVar.
globals :: IORef (QSem, Vector Sched)
globals = unsafePerformIO $ do
  when dbg $ printf "Initializing global structures\n"
  n <- getNumCapabilities
  sem <- newQSem 0
  -- make a Vector of Schedulers, one for each capability
  v <- V.generateM n $ \i ->
    Sched i <$> newHotVar emptydeque <*> (newStdGen >>= newHotVar)
  -- pin a thread to each capability, and have it wait on the idle semaphore
  V.forM_ v $ \(Sched {no}) -> forkOn no $ do
    when dbg $ printf "[%d] Spawning thread on capability\n" no
    workerLoop
  newIORef (sem, v)

{-# INLINE idleSem #-}
-- | Retrieve the global idle semaphore
idleSem :: IO QSem
idleSem = fst <$> readIORef globals

{-# INLINE allScheds #-}
-- | Retrieve the vector of schedulers
allScheds :: IO (Vector Sched)
allScheds = snd <$> readIORef globals

{-# INLINE getSched #-}
-- | Retrieve a particular scheduler by capability num
getSched :: Int -> IO Sched
getSched i = do
  v <- allScheds
#ifdef DEBUG
  case v V.!? i of
    Just s -> return s
    Nothing -> error $ printf "[%d] Couldn't find scheduler\n" i
#else
  return $ v V.! i
#endif

{-# INLINE mySched #-}
-- | Retrieve the scheduler for the current thread.
mySched :: IO Sched
mySched = do
  (i, _) <- threadCapability =<< myThreadId
  getSched i

--------------------------------------------------------------------------------
-- Popping and pushing work

{-# INLINE popWork #-}
-- | Attempt to take work off the current thread's queue.
popWork :: IO (Maybe (Par ()))
popWork = do
  (Sched { workpool, no }) <- mySched
  mb <- modifyHotVar workpool takefront
  if dbg 
    then case mb of 
           Nothing -> return Nothing
	   Just x  -> do 
             sn <- makeStableName mb
	     printf " [%d] POP work unit %d\n" no (hashStableName sn)
	     return mb
    else return mb

{-# INLINE pushWork #-}
-- | Push work onto the queue of the specified thread. /Note:/ this is
-- the /only/ place where the 'idleSem' is signalled.
pushWork :: Int -> Par () -> IO ()
pushWork i task = do
  v <- allScheds
  case v V.!? i of
    Just (Sched { workpool }) -> do
      when dbg $ do sn <- makeStableName task
                    printf " [%d] PUSH work unit %d\n" i (hashStableName sn)
      modifyHotVar_ workpool (addfront task)
      signalQSem =<< idleSem
    _ -> do (me, _) <- threadCapability =<< myThreadId
            error $ printf " [%d] Tried to push onto nonexistend thread %d\n" me i

--------------------------------------------------------------------------------
-- Worker loop and local stealing

workerLoop :: IO ()
workerLoop = do
  (Sched { workpool, no }) <- mySched
  -- first, wait until there is work to do
  when dbg $ printf "[%d] Entering worker loop\n" no
  waitQSem =<< idleSem
  -- then, try taking work off own queue
  mtask <- popWork
  case mtask of
    -- if own queue is empty, steal, then reenter workerLoop
    Nothing -> steal >> workerLoop
    -- otherwise, run popped work, then reenter workerLoop
    Just task -> do
      when dbg $ do sn <- makeStableName task
                    printf " [%d] popped work %d from own queue\n" 
                      no (hashStableName sn)
      C.runContT (unPar task) $ \() -> do
        when dbg $ printf " [%d] finished work\n" no
        workerLoop

-- | Unconditionally return to the 'workerLoop'
reschedule :: Par a
reschedule = Par $ C.ContT (\_ -> workerLoop)

rand :: HotVar StdGen -> IO Int
rand ref = 
 do g <- readHotVar ref
    let (n,g') = next g
	i = n `mod` numCapabilities
    writeHotVar ref g'
    return i

steal :: IO ()
steal = do
  (Sched { no=me, rng }) <- mySched
  when dbg $ printf " [%d] stealing\n" me
  let getNext :: IO Int
      getNext = rand rng
      loop :: Int -> IO ()
      loop i | i == me   = loop =<< getNext
             | otherwise = do
        (Sched { workpool, no=target }) <- getSched i
        when dbg $ printf " [%d] trying steal from %d\n" me target
        -- try and take off the end of the target's workpool
        mtask <- modifyHotVar workpool takeback
        case mtask of
          -- no work found; try another random
          Nothing   -> loop =<< getNext
          -- found work; perform it, then return () to reenter workerLoop
          Just task -> do
            when dbg $ do sn <- makeStableName task
                          printf " [%d]  | stole work (unit %d) from cpu %d\n" 
                            me (hashStableName sn) target
            C.runContT (unPar task) $ \() -> do
              when dbg $ printf " [%d] finished stolen work from %d\n" me target
  loop =<< getNext

{-# INLINE new #-}
-- | Create a new, empty 'IVar'
new :: Par (IVar a)
new = liftIO $ IVar <$> newIORef Empty

{-# INLINE get #-}
-- | Read the value in an 'IVar'. If the 'IVar' contains 'Empty', the
-- current computation will be suspended until the 'IVar' is filled by
-- a 'put'.
get iv@(IVar v) = callCC $ \cont -> do
  contents <- liftIO $ readIORef v
  case contents of
    -- If the IVar is full, just return the value
    Full a -> return a
    -- Otherwise, potentially suspend and reschedule
    _ -> do
      let resched = do
            when dbg $ do 
              (Sched { no }) <- liftIO mySched
              sn <- liftIO $ makeStableName iv
              liftIO $ printf " [%d] Rescheduling on unavailable IVar %d\n"
                         no (hashStableName sn)
            reschedule
      r <- liftIO $ atomicModifyIORef v $ \contents ->
             case contents of
               -- Still empty, so save cont and reschedule
               Empty      -> (Blocked [cont], resched)
               -- Someone filled while we were thinking; return value
               Full a     -> (Full a, return a)
               -- Other continuations waiting; add to list and reschedule
               Blocked cs -> (Blocked (cont:cs), resched)
      r

{-# INLINE put_ #-}
-- | Like 'put', but head-strict rather than fully-strict
put_ iv@(IVar v) !a = liftIO $ do
  (Sched { no }) <- mySched
  -- put the value, returning a list of waiting continuations, or
  -- throwing an error if the IVar is already full
  cs <- atomicModifyIORef v $ \contents ->
          case contents of
            Empty      -> (Full a, [])
            Full _     -> error "Multiple put into IVar"
            Blocked cs -> (Full a, cs)
#ifdef DEBUG
  sn <- makeStableName iv
  printf " [%d] Put value %s into IVar %d. Waking up %d continuations.\n"
    no (show a) (hashStableName sn) (length cs)
#endif
  -- apply each waiting continuation to the value, then queue up the
  -- resulting Par computations
  mapM_ (pushWork no . ($a)) cs        

-- TODO: Ask about continuation stealing version
{-# INLINE fork #-}
fork :: Par () -> Par ()
#ifdef FORKPARENT
#warning "FORK PARENT POLICY USED"
fork task = do 
   (Sched { no }) <- liftIO mySched
   callCC$ \parent -> do
      let wrapped = parent ()
      -- Is it possible to slip in a new Sched here?
      -- let wrapped = lift$ R.runReaderT (parent ()) undefined
      liftIO $ pushWork no wrapped
      -- Then execute the child task and return to the scheduler when
      -- it is complete:
      task 
      -- If we get to this point we have finished the child task:
      reschedule -- We reschedule to [maybe] pop the cont we pushed.
      liftIO $ putStrLn " !!! ERROR: Should not reach this point #1"   
   when dbg$ do 
    (Sched { no=sched2 }) <- liftIO mySched
    liftIO $ printf " [%d] Parent cont invoked (originally from %d)\n" no sched2
#else
fork task = do
   (Sched { no }) <- liftIO mySched
   when dbg $ liftIO $ printf " [%d] forking task...\n" no
   liftIO $ pushWork no task
#endif

-- | Entry point for the nested, local Par scheduler. Puts the
-- supplied 'Par' computation on the work queue for the scheduler of
-- the thread entering here. Note that this is [probably] different
-- than the thread created for this CPU by 'forkOn', but this
-- [probably] shouldn't matter.
runParIO userComp = do
  (Sched { no }) <- mySched
  -- Make a new MVar to store the final answer of /this/ computation
  m <- newEmptyMVar
  -- Wrap the user computation in code to extract the final answer
  let wrappedComp = do
        when dbg $ do
          (Sched { no }) <- liftIO $ mySched
          liftIO $ printf " [%d] Starting user computation\n" no
        ans <- userComp
        when dbg $ do
          (Sched { no }) <- liftIO $ mySched
          liftIO $ printf " [%d] Finished user computation, writing MVar\n" no
        liftIO $ putMVar m ans
  -- Push work, which signals the idle semaphore so a worker will wake up
  pushWork no wrappedComp
  ans <- takeMVar m
  when dbg $ do
    (Sched { no=final }) <- mySched
    printf " [%d] Finished user computation started by %d\n" final no
  return ans

runParDist = undefined

{-# INLINE runPar #-}
runPar = unsafePerformIO . runParIO

--------------------------------------------------------------------------------
-- <boilerplate>

-- The following is usually inefficient! 
newFull_ a = do v <- new
		put_ v a
		return v

newFull a = deepseq a (newFull_ a)

{-# INLINE put  #-}
put v a = deepseq a (put_ v a)

spawn p  = do r <- new;  fork (p >>= put r);   return r
spawn_ p = do r <- new;  fork (p >>= put_ r);  return r
spawnP a = spawn (return a)

-- In Debug mode we require that IVar contents be Show-able:
#ifdef DEBUG
put    :: (Show a, NFData a) => IVar a -> a -> Par ()
spawn  :: (Show a, NFData a) => Par a -> Par (IVar a)
spawn_ :: Show a => Par a -> Par (IVar a)
spawnP :: (Show a, NFData a) => a -> Par (IVar a)
put_   :: Show a => IVar a -> a -> Par ()
get    :: Show a => IVar a -> Par a
newFull :: (Show a, NFData a) => a -> Par (IVar a)
newFull_ ::  Show a => a -> Par (IVar a)
runPar   :: Show a => Par a -> a
runParIO :: Show a => Par a -> IO a
runParDist :: Show a => Par a -> ProcessM a
#else
spawn      :: NFData a => Par a -> Par (IVar a)
spawn_     :: Par a -> Par (IVar a)
spawnP     :: NFData a => a -> Par (IVar a)
put_       :: IVar a -> a -> Par ()
put        :: NFData a => IVar a -> a -> Par ()
get        :: IVar a -> Par a
runPar     :: Par a -> a
runParIO   :: Par a -> IO a
runParDist :: Par a -> ProcessM a 
newFull    :: NFData a => a -> Par (IVar a)
newFull_   :: a -> Par (IVar a)


instance PC.ParFuture Par IVar where
  get    = get
  spawn  = spawn
  spawn_ = spawn_
  spawnP = spawnP

instance PC.ParIVar Par IVar where
  fork = fork
  new  = new
  put_ = put_
  newFull = newFull
  newFull_ = newFull_

#endif

instance Functor Par where
   fmap f xs = xs >>= return . f

instance Applicative Par where
   (<*>) = ap
   pure  = return
-- </boilerplate>
--------------------------------------------------------------------------------
