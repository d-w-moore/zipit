# zipit

This repo contains two scripts useful for gzipping and checking large files 
as quickly as possible leveraging the parallelism of your machine.

They require only that python be installed, and they depend only on the 
modules included as part of the Python standard library.  (Naturally this
includes the zlib and gzip libraries - which may not be part of a default
OS install for minimal or containerized distributions).

## zipit.py

Example uses:

```
   ./zipit -v large.tar                 # => creates large.tar.gz with the default parallelism 
                                        #    (& verbosely informs of the piece-wise gzip tasks)

   ./zipit -qm large.tar                # => creates large.tar.gz using all available CPU's

   some_command | ./zipit - > out.gz    # => gzips from the stdin stream, onto stdout

   docker export cimg | \
     ./zipit.py -d cimg.dig - >cimg.tgz # => gzips from the stdin stream, onto stdout
```

## testzip.py

Examples use (for context, see the final `zipit.py` example above):

```
   ./testzip.py cimg.tgz cimg.dig      # => tests the gzipped file's integrity using a digest file
                                       #    (returns 0 if the integrity is good)
```
