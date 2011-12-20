## HOWTO: Distributed Monad Par 

This package contains an extension to the `monad-par` interface:

    longSpawn :: (NFData a, Serializable a) => Closure (Par a) -> Par (IVar a)
    runParDist :: Maybe FilePath -> [RemoteCallMetaData] -> Par a -> IO a
    
The `Serializable` constraint and the `Closure` type constructors are
the additions from the supporting CloudHaskell (`remote`) package that
enable computations to be executed on distributed resources.

The `runParDist` type signature is reminiscent of CloudHaskell's
[`remoteInit`](http://hackage.haskell.org/packages/archive/remote/0.1.1/doc/html/Remote-Init.html),
and the first two arguments are indeed just passed through to that
function. 

### Running a distributed example

In the `examples/distributed/` directory, there are several relevant files:

  * `parfib_dist.hs`, an implementation of the exponential fibonacci
    algorithm that uses `longSpawn` for half of its recursive calls
  * `builddist.sh`, a shell script that builds the `parfib_dist`
    executable, and then copies a version to the `worker/`
    subdirectory
  * `rundist.sh`, a shell script that first launches the worker
    executable, and then launches the master, at which point the
    computation begins
  * `config` and `worker/config`, CloudHaskell configuration files
    that establish MASTER and WORKER node roles for the two copies of
    the executable respectively. See the
    [CloudHaskell example config](https://github.com/jepst/CloudHaskell/blob/master/examples/tests/config)
    for details of what can be specified here.
    
To run the example:

  1. In the root directory of the `monad-par` source tree, run `cabal install`.
  2. Run `cd examples/distributed`.
  3. Run `./builddist.sh`.
  4. Run `./rundist.sh`.
  
### Creating a distributed monad-par program

The `longSpawn` function expects a CloudHaskell `Closure` type as its
first argument, so any computations which you want to pass to
`longSpawn` must meet the CloudHaskell requirements for being a
`Closure`; see
http://hackage.haskell.org/packages/archive/remote/0.1.1/doc/html/Remote-Call.html
for details.

#### Creating a Par Closure

Ordinarily, a `Closure` is created with a Template Haskell splice
`remotable`, which derives the boilerplate code necessary for handling
marshalling of arguments and registration of the underlying function
in the CloudHaskell closure registry. Pending an extension to this
facility, the functions to produce closures containing `Par`
computations must be produced by hand.

See the `examples/distributed/parfib_dist.hs` source for an example of
this hand-modified code. For the adventurous, it was derived by
compiling a dummy version of the function with the return type
switched from `Par FibType` to `IO FibType`. With the `-ddump-splices`
flag enabled, the compiler outputs the code for creating an `IO`
closure. From there, the types are changed and a call to `runParIO` is
added to create the final closure.

