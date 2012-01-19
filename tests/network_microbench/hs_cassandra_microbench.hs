
-- Peculiar:
--    * Requires hscassandra 0.0.8, available on github only

-- Functions to:
--    * Connect to a cassandra nosql server
--    * Put a bytestring
--    * Get a bytestring

-- This file acts as a microbench for insertion or retrieval of keys

-- Prerequisite:
--   Create keyspace Test2 with the commands:
--    cassandra-cli --host localhost
--    [default@unknown] create keyspace Test2;
--    [default@unknown] use Test2;
--    [default@Keyspace1] create column family Users with comparator=UTF8Type and default_validation_class=UTF8Type and key_validation_class=UTF8Type;

import Database.Cassandra
import Database.Cassandra.Types
import qualified Data.ByteString.Lazy.Char8 as BS
import qualified Database.Cassandra.Thrift.Cassandra_Types as CT
import Data.Either.Utils
import qualified Data.List as L
import System.Time
import Data.Time.Clock
import System.Environment
import Control.Monad
import qualified Data.Set as S

keyspace = "Users"

config = CassandraConfig { cassandraKeyspace = "Test2"
                     , cassandraConsistencyLevel = ONE
                     , cassandraHostname = "127.0.0.1"
                     , cassandraPort = 9160
                     , cassandraUsername = ""
                     , cassandraPassword = ""
                     }

pack :: [Char] -> BS.ByteString
pack string = BS.pack string
     
deColumnValue :: Column -> BS.ByteString
deColumnValue (Column _ value _) = value
                 
fetchValue string = runCassandraT config $ do
  res <- get keyspace (pack string) AllColumns
  return $ deColumnValue $ head $ res

insertValue string byteString = runCassandraT config $ do
  insert keyspace (pack string) [ (pack "name") =: byteString ]
  
removeValue string = runCassandraT config $ do
  remove keyspace (pack string) (ColNames [ (pack "name") ])

main = do

  let size = 5000  
  --input <- readFile "/usr/share/dict/words"
  --let words = tail $ lines input
  let words = map (\n -> (show n) ++ "Thequickbrownfoxjumpedoverthelazydog")  $ [1..size]
  
  putStrLn "Benchmarking Cassandra bindings by writing in every dictionary file word as a k,v pair."
  
  putStrLn$ "About to start timing "++show size++" writes..."
  writeStart <- getCurrentTime
  mapM_ (\w -> insertValue w (pack w)) words
  writeStop <- getCurrentTime
  putStrLn ("Writes took " ++ (show $ diffUTCTime writeStop writeStart))
  
  putStrLn "Starting reads..."  
  readStart <- getCurrentTime
  mapM_ (\w -> fetchValue w) words
  readStop <- getCurrentTime
  putStrLn ("Reads took "  ++ (show $ diffUTCTime readStop readStart))
  putStrLn ("Total time: " ++ (show $ diffUTCTime readStop writeStart))
  
  putStrLn "Removing keys..."
  
  mapM_ (\w -> removeValue w) words
  putStrLn "Done."
