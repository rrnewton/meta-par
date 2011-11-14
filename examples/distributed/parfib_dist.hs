{-# LANGUAGE CPP #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -O2 -ddump-splices #-}
import Data.Int
import System.Environment
import GHC.Conc
import Control.Applicative
-- import Control.Monad.Par
--import Control.Monad.Par.Scheds.ContFree
import Control.Monad.Par.Scheds.DistDirect

import Remote

-- necessary imports for remotable-generated code
import Control.Monad.IO.Class
import Remote.Closure
import Remote.Encoding
import Remote.Reg

type FibType = Int64

-- sequential version of the code
fib :: FibType -> FibType
fib 0 = 1
fib 1 = 1
fib x = fib (x-2) + fib (x-1)

-- IO version of fib for remotable TH testing
fibIO :: FibType -> IO FibType
fibIO 0 = return 1
fibIO 1 = return 1
fibIO x = (+) <$> fibIO (x-2) <*> fibIO (x-1)

-- remotable ['fibIO]



fibPar :: FibType -> Par FibType
fibPar 0 = return 1
fibPar 1 = return 1
fibPar x = (+) <$> fibPar (x-2) <*> fibPar (x-1)

--------------------------------------------------------------------------------
-- Hand-tweaked Closure and RemoteCallMetaData code

parfib1__0__impl :: Payload -> IO FibType
parfib1__0__impl a
  = do res <- liftIO (Remote.Encoding.serialDecode a)
       case res of 
         Just a1 -> liftIO $ runParIO (parfib1 a1)
         _ -> error "Bad decoding in closure splice of parfib1"

parfib1__0__implPl :: Payload -> IO Payload
parfib1__0__implPl a
  = do res <- parfib1__0__impl a
       liftIO (Remote.Encoding.serialEncode res)

parfib1__closure :: FibType -> Closure (Par FibType)
parfib1__closure
  = \ a1
      -> Remote.Closure.Closure
           "Main.parfib1__0__impl" (Remote.Encoding.serialEncodePure a1)

__remoteCallMetaData :: RemoteCallMetaData
__remoteCallMetaData x
  = Remote.Reg.putReg
      parfib1__0__impl
      "Main.parfib1__0__impl"
      (Remote.Reg.putReg
         parfib1__0__implPl
         "Main.parfib1__0__implPl"
         (Remote.Reg.putReg parfib1__closure "Main.parfib1__closure" x))

-- Par monad version:
parfib1 :: FibType -> Par FibType
parfib1 n | n < 2 = return 1
parfib1 n = do 
    xf <- longSpawn $ parfib1__closure (n-1)
    y  <-             parfib1 (n-2)
    x  <- get xf
    return (x+y)

{-
-- Par monad version, with threshold:
parfib1B :: FibType -> FibType -> Par FibType
parfib1B n c | n <= c = return $ fib n
parfib1B n c = do 
    xf <- longSpawn $ parfib1B (n-1) c
    y  <-             parfib1B (n-2) c
    x  <- get xf
    return (x+y)
-}

main = do 
    args <- getArgs
    let (version, size, cutoff) = case args of 
            []      -> ("monad", 20, 1)
            [v]     -> (v,       20, 1)
            [v,n]   -> (v, read n,   1)
            [v,n,c] -> (v, read n, read c)

    case version of 
        "monad"  -> do 
		if cutoff == 1 
                  then do putStrLn "Using non-thresholded version:"
                          ans <- (runParDist (Just "config") 
                                                 [__remoteCallMetaData]
                                                 (parfib1 size) :: IO FibType)
                          putStrLn $ "Final answer: " ++ show ans
		  else    undefined -- print <$> (runParDist $ parfib1B size cutoff :: IO FibType)
        -- TEMP: force thresholded version even if cutoff==1
        "thresh" -> undefined -- print <$> (runParDist $ parfib1B size cutoff :: IO FibType)
        _        -> error $ "unknown version: "++version

