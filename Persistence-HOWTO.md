
This is some progress so far on the persistence. LongSpawn has been modified to include
a communication via channel to the cassandraDaemon, which will output CloudHaskell 
"Payload"s to Cassandra. A verification daemon then checks to see if after reading
the paylaod back in from Cassandra it is still equal to the original payload.

Steps needed to get dist-direct-persist to run:

1) Download apache-cassandra from the internet. This shouldn't require an install?

2) Start Cassandra using the _/bin/cassandra command.

3) Start Cassandra-cli using the _/bin/cassandra-cli command.

4) Now we need to make the appropriate keyspace and columns for monad par.
   Run the following commands:

   a) connect localhost/9160;
   b) create keyspace distDirectPersist;
   c) use distDirectPersist;
   d) create column family 120 with comparator = UTF8Type;

5) Install hscassandra-0.0.8 from github.

   a) https://github.com/necrobious/hscassandra is the url
   b) I had to (with ghc 7.2.1) modify the hscassandra.cabal file to require
      a bytestring version >= 0 to get this to install. Very possible you will not
      need to do so.

6) Use cabal-dev to install the monad-par branch dist-direct-persist

   This is where all kinds of stuff can go wrong.

7) If everything is successful up to this point you *should* be able to run
   the ditributed example parFib and have things work.

   a) cd to the examples/distributed directory
   b) ./builddist.sh
   c) run provided command in ./worker
   d) run provided command in .

If everything works you should see a stream of print statements notifying
you that ivars are being persisted, and then shortly after verification print
statements assuring you that it actually worked. 
