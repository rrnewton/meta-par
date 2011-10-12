{-# LANGUAGE RankNTypes, NamedFieldPuns, BangPatterns,
             ExistentialQuantification, CPP, ScopedTypeVariables
	     #-}
{-# OPTIONS_GHC -Wall -fno-warn-name-shadowing -fno-warn-unused-do-bind #-}

-- | This module exposes the internals of the @Par@ monad so that you
-- can build your own scheduler or other extensions.  Do not use this
-- module for purposes other than extending the @Par@ monad with new
-- functionality.

module Control.Monad.Par.Scheds.TraceInternal (
   Trace(..), Sched(..), Par(..),
   IVar(..), IVarContents(..),
   sched,
   runPar, runParAsync, runParAsyncHelper,
   new, newFull, newFull_, get, put_, put,
   pollIVar, yield,
 ) where


import Control.Monad as M hiding (mapM, sequence, join)
import Prelude hiding (mapM, sequence, head,tail)
import Data.IORef
import System.IO.Unsafe
import Control.Concurrent hiding (yield)
import GHC.Conc hiding (yield)
import Control.DeepSeq
import Control.Applicative
-- import Text.Printf

import Remote
import Control.Monad.IO.Class

-- ---------------------------------------------------------------------------

newHotVar :: MonadIO m => a -> m (HotVar a)
modifyHotVar :: MonadIO m => HotVar a -> (a -> (a, b)) -> m b
writeHotVar :: MonadIO m => HotVar a -> a -> m ()
readHotVar :: MonadIO m => HotVar a -> m a
{-# INLINE newHotVar #-}
{-# INLINE modifyHotVar #-}
{-# INLINE writeHotVar #-}
{-# INLINE readHotVar #-}

-- | Using STM HotVars from RRN's Direct.hs for now
type HotVar a = TVar a
newHotVar = liftIO . newTVarIO
modifyHotVar  tv fn = liftIO $ atomically (do x <- readTVar tv 
				              let (x2,b) = fn x
				              writeTVar tv x2
				              return b)
modifyHotVar_ tv fn = liftIO $ atomically (do x <- readTVar tv 
                                              writeTVar tv (fn x))
readHotVar x = liftIO . atomically $ readTVar x
writeHotVar v x = liftIO . atomically $ writeTVar v x

instance Show (TVar a) where 
  show ref = "<tvar>"

hotVarTransaction = atomically
readHotVarRaw  = readTVar
writeHotVarRaw = writeTVar

-- ---------------------------------------------------------------------------

data Trace = forall a . Get (IVar a) (a -> Trace)
           | forall a . Put (IVar a) a Trace
           | forall a . New (IVarContents a) (IVar a -> Trace)
           | Fork Trace Trace
           | Done
           | Yield Trace

-- | The main scheduler loop.
sched :: MonadIO m => Bool -> Sched -> Trace -> m ()
sched _doSync queue t = loop t
 where 
  loop t = case t of
    New a f -> do
      r <- newHotVar a
      loop (f (IVar r))
    Get (IVar v) c -> do
      e <- readHotVar v
      case e of
         Full a -> loop (c a)
         _other -> do
           r <- modifyHotVar v $ \e -> case e of
                        Empty    -> (Blocked [c], reschedule queue)
                        Full a   -> (Full a,      loop (c a))
                        Blocked cs -> (Blocked (c:cs), reschedule queue)
           r
    Put (IVar v) a t  -> do
      cs <- modifyHotVar v $ \e -> case e of
               Empty    -> (Full a, [])
               Full _   -> error "multiple put"
               Blocked cs -> (Full a, cs)
      mapM_ (pushWork queue. ($a)) cs
      loop t
    Fork child parent -> do
         pushWork queue child
         loop parent
    Done ->
         if _doSync
	 then reschedule queue
-- We could fork an extra thread here to keep numCapabilities workers
-- even when the main thread returns to the runPar caller...
         else do liftIO $ putStrLn " [par] Forking replacement thread..\n"
                 liftIO $ forkIO (reschedule queue); return ()
-- But even if we don't we are not orphaning any work in this
-- threads work-queue because it can be stolen by other threads.
--	 else return ()

    Yield parent -> do 
        -- Go to the end of the worklist:
        let Sched { workpool } = queue
        -- TODO: Perhaps consider Data.Seq here.
	-- This would also be a chance to steal and work from opposite ends of the queue.
        modifyHotVar workpool $ \ts -> (ts++[parent], ())
	reschedule queue

-- | Process the next item on the work queue or, failing that, go into
--   work-stealing mode.
reschedule :: MonadIO m => Sched -> m ()
reschedule queue@Sched{ workpool } = do
  e <- modifyHotVar workpool $ \ts ->
         case ts of
           []      -> ([], Nothing)
           (t:ts') -> (ts', Just t)
  case e of
    Nothing -> steal queue
    Just t  -> sched True queue t


-- RRN: Note -- NOT doing random work stealing breaks the traditional
-- Cilk time/space bounds if one is running strictly nested (series
-- parallel) programs.

-- | Attempt to steal work or, failing that, give up and go idle.
steal :: MonadIO m => Sched -> m ()
steal q@Sched{ idle, scheds, no=my_no } = do
  -- printf "cpu %d stealing\n" my_no
  go scheds
  where
    go [] = do m <- liftIO newEmptyMVar
               r <- modifyHotVar idle $ \is -> (m:is, is)
               if length r == numCapabilities - 1
                  then do
                     -- printf "cpu %d initiating shutdown\n" my_no
                     mapM_ (\m -> liftIO $ putMVar m True) r
                  else do
                    done <- liftIO $ takeMVar m
                    if done
                       then do
                         -- printf "cpu %d shutting down\n" my_no
                         return ()
                       else do
                         -- printf "cpu %d woken up\n" my_no
                         go scheds
    go (x:xs)
      | no x == my_no = go xs
      | otherwise     = do
         r <- modifyHotVar (workpool x) $ \ ts ->
                 case ts of
                    []     -> ([], Nothing)
                    (x:xs) -> (xs, Just x)
         case r of
           Just t  -> do
              -- printf "cpu %d got work from cpu %d\n" my_no (no x)
              sched True q t
           Nothing -> go xs

-- | If any worker is idle, wake one up and give it work to do.
pushWork :: MonadIO m => Sched -> Trace -> m ()
pushWork Sched { workpool, idle } t = do
  modifyHotVar workpool $ \ts -> (t:ts, ())
  idles <- readHotVar idle
  when (not (null idles)) $ do
    r <- modifyHotVar idle (\is -> case is of
                                     [] -> ([], return ())
                                     (i:is) -> (is, liftIO $ putMVar i False))
    r -- wake one up

data Sched = Sched
    { no       :: {-# UNPACK #-} !Int,
      workpool :: HotVar [Trace],
      idle     :: HotVar [MVar Bool],
      scheds   :: [Sched] -- Global list of all per-thread workers.
    }
--  deriving Show

newtype Par a = Par {
    runCont :: (a -> Trace) -> Trace
}

instance Functor Par where
    fmap f m = Par $ \c -> runCont m (c . f)

instance Monad Par where
    return a = Par ($ a)
    m >>= k  = Par $ \c -> runCont m $ \a -> runCont (k a) c

instance Applicative Par where
   (<*>) = ap
   pure  = return

newtype IVar a = IVar (HotVar (IVarContents a))
-- data IVar a = IVar (IORef (IVarContents a))

-- Forcing evaluation of a IVar is fruitless.
instance NFData (IVar a) where
  rnf _ = ()


-- From outside the Par computation we can peek.  But this is nondeterministic.
pollIVar :: MonadIO m => IVar a -> m (Maybe a)
pollIVar (IVar ref) = 
  do contents <- readHotVar ref
     case contents of 
       Full x -> return (Just x)
       _      -> return (Nothing)


data IVarContents a = Full a | Empty | Blocked [a -> Trace]


{-# INLINE runPar_internal #-}
runPar_internal :: MonadIO m => Bool -> Par a -> m a
runPar_internal _doSync x = do
   workpools <- replicateM numCapabilities $ newHotVar []
   idle <- newHotVar []
   let states = [ Sched { no=x, workpool=wp, idle, scheds=states }
                | (x,wp) <- zip [0..] workpools ]

#if __GLASGOW_HASKELL__ >= 701 /* 20110301 */
    --
    -- We create a thread on each CPU with forkOnIO.  The CPU on which
    -- the current thread is running will host the main thread; the
    -- other CPUs will host worker threads.
    --
    -- Note: GHC 7.1.20110301 is required for this to work, because that
    -- is when threadCapability was added.
    --
   (main_cpu, _) <- threadCapability =<< myThreadId
#else
    --
    -- Lacking threadCapability, we always pick CPU #0 to run the main
    -- thread.  If the current thread is not running on CPU #0, this
    -- will require some data to be shipped over the memory bus, and
    -- hence will be slightly slower than the version above.
    --
   let main_cpu = 0
#endif

   m <- liftIO newEmptyMVar
   forM_ (zip [0..] states) $ \(cpu,state) -> 
        liftIO . forkOnIO cpu $
          if (cpu /= main_cpu)
             then reschedule state
             else do
                  rref <- newHotVar Empty :: IO (HotVar (IVarContents a))
                  sched _doSync state $ runCont (x >>= put_ (IVar rref)) (const Done)
                  readHotVar rref >>= putMVar m

   r <- liftIO $ takeMVar m
   case r of
     Full a -> return a
     _ -> error "no result"


runPar :: Par a -> a
runPar = unsafePerformIO . runPar_internal True

-- | An asynchronous version in which the main thread of control in a
-- Par computation can return while forked computations still run in
-- the background.  
runParAsync :: Par a -> a
runParAsync = unsafePerformIO . runPar_internal False

-- | An alternative version in which the consumer of the result has
-- | the option to "help" run the Par computation if results it is
-- | interested in are not ready yet.
runParAsyncHelper :: MonadIO m => Par a -> (a, m ())
runParAsyncHelper = undefined -- TODO: Finish Me.

-- -----------------------------------------------------------------------------

-- | creates a new @IVar@
new :: Par (IVar a)
new  = Par $ New Empty

-- | creates a new @IVar@ that contains a value
newFull :: NFData a => a -> Par (IVar a)
newFull x = deepseq x (Par $ New (Full x))

-- | creates a new @IVar@ that contains a value (head-strict only)
newFull_ :: a -> Par (IVar a)
newFull_ !x = Par $ New (Full x)

-- | read the value in a @IVar@.  The 'get' can only return when the
-- value has been written by a prior or parallel @put@ to the same
-- @IVar@.
get :: IVar a -> Par a
get v = Par $ \c -> Get v c

-- | like 'put', but only head-strict rather than fully-strict.
put_ :: IVar a -> a -> Par ()
put_ v !a = Par $ \c -> Put v a (c ())

-- | put a value into a @IVar@.  Multiple 'put's to the same @IVar@
-- are not allowed, and result in a runtime error.
--
-- 'put' fully evaluates its argument, which therefore must be an
-- instance of 'NFData'.  The idea is that this forces the work to
-- happen when we expect it, rather than being passed to the consumer
-- of the @IVar@ and performed later, which often results in less
-- parallelism than expected.
--
-- Sometimes partial strictness is more appropriate: see 'put_'.
--
put :: NFData a => IVar a -> a -> Par ()
put v a = deepseq a (Par $ \c -> Put v a (c ()))

-- | Allows other parallel computations to progress.  (should not be
-- necessary in most cases).
yield :: Par ()
yield = Par $ \c -> Yield (c ())

