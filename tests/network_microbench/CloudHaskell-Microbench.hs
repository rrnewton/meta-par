{-# LANGUAGE DeriveDataTypeable,TemplateHaskell #-}
module Main where

-- A convoluted example showing some different ways
-- messages can be passed between processes.

import Remote

import Data.Typeable (Typeable)
import Data.Data (Data)
import Data.Binary (Binary,get,put)
import System.Random (randomR,getStdRandom)
import Control.Concurrent (threadDelay)
import Control.Monad (when)
import Control.Monad.Trans (liftIO)

import System.Time
import Data.Time.Clock
import System.Environment
import System ( getArgs )
import System.Console.GetOpt

data Chunk = Chunk Int deriving (Typeable,Data)

data Str = Str [Char] deriving (Typeable,Data)

instance Binary Str where
  get = genericGet
  put = genericPut

instance Binary Chunk where
    get = genericGet
    put = genericPut

-- Receives N messages and then tells the master it has finished
responder :: Int -> ProcessId -> ProcessM ()
responder msgNum master = do
  receiveLoop msgNum
    where
      receiveLoop 0 = send master (Chunk 1)
      receiveLoop n = 
        let resultMatch = match (\(Str x) -> return x)
          in do { msg <- receiveWait [resultMatch]
                ; receiveLoop (n-1) }

$( remotable ['responder] )

-- create a message list
makeMessages :: Int -> Int -> [String]
makeMessages msgNum msgSize =
  [replicate msgSize 'c' | n <- [1..msgNum]]

initialProcess :: Int -> Int -> String -> ProcessM ()
initialProcess msgNum msgSize "SENDER" =
  do peers <- getPeers
     mypid <- getSelfPid
     let responders = findPeerByRole peers "RESPONDER"
         
     responderPIDS <- mapM (\responder -> spawn responder (responder__closure msgNum mypid)) responders
     
     let dest = head responderPIDS
         messages = makeMessages msgNum msgSize
     
     --let startTime = return getCurrentTime
     mapM_ (\str -> send dest (Str str)) messages
     d <- receiveWait [match (\(Chunk d) -> return d)]
     --let endTime = return getCurrentTime
     --say ("For message size " ++ show msgSize ++ " and number " ++ show msgNum ++ " total time was: " ++ show (diffUTCTime endTime startTime))
     say "Finished"

initialProcess _ _ "RESPONDER" = receiveWait []

main = do
  
  args <- getArgs
  let num = read $ head args :: Int
  let size = read $ head $ tail args :: Int
  
  putStrLn "About to run Microbench of Cloud Haskell network messaging"
  putStrLn ("Message number: " ++ show num)
  putStrLn ("Message size (characters) : " ++ show size)
  
  remoteInit (Just "config") [Main.__remoteCallMetaData] (initialProcess num size)

