#!/usr/bin/env python2
from __future__ import print_function
import tempfile
import multiprocessing
import sys, os
import gzip
import hashlib
import getopt
import Queue

# gzips a file or stream in parallel
# takes maximum advantage of the CPU's available:
#
# $ cmd > cmdout ; python zipit.py cmdout
#
# ( produces cmdout.gz ).
#
# In Bash, pipe stdin/stdout and use TMPDIR and verbose option:
#
# $ chmod +x ./zipit.py
# $ cmd | TMPDIR="/dev/shm" ./zipit.py -v - >cmdout.gz
#
# Lots of space is needed for temporary file chunks
# during the gzip. These are stored and zipped in
# /tmp by default (or optionally, wherever TMPDIR points)
# and are cleaned up unless -k is applied.

cmprlvl = 4
verbosity = 0
keep = False
digest = ''

Qmax = 6
qu=Queue.Queue()

dig = {}

def usage():
  print('''
  Usage: {0} [-c level] [-{{k|v|h|q<N>|d<Filename>}} ...] {{-|file_to_compress}}
  Options: -c      compression level (defaults to {cmprlvl})
           -d fn   digest filename
           -q N | 'm' set parallelism ( up to machines cpu count)
                      ('m' specifies maximum, and the default is {Qmax})
           -k      keep files around - for debug purposes only
           -h      help '''.format(sys.argv[0],**globals()))
  exit()

opt,arg = getopt.getopt(sys.argv[1:],'c:hkvd:q:')

for key,val in opt:
    if   key == '-c': cmprlvl = int(val)
    elif key == '-q': Qmax = int(val)
    elif key == '-h': usage()
    elif key == '-d': digest = val
    elif key == '-k': keep=True
    elif key == '-v': verbosity=1

Qmax = min( max( Qmax, 0 ), multiprocessing.cpu_count() )
pid = os.getpid()
d = {}
tmp_dir = os.getenv('TMPDIR','/tmp')

class GZipper(multiprocessing.Process):
    def __repr__(self):
        return '{!s}({!r})'.format(self.__class__.__name__,self.n)
    def __init__(self,t):
        self.n = t.name
        self.n_out = self.n + '.gz'
        d[self.n] = t
        super(GZipper,self).__init__()
    def __del__(self):
        del d[self.n]
    def run(self):
        with open(self.n,'rb') as f:
            g=gzip.GzipFile(filename=self.n + '.gz',mode='wb',compresslevel=cmprlvl)
            while True:
                b=f.read(1024**2)
                if b: g.write(b)
                else: break

chunk=200*1024**2
i = 0
last = False

if __name__ == '__main__':
    p = []
    try: iname = arg[0]
    except: usage()
    dg = open(digest,'w') if digest else None

    with (open(iname+'.gz','wb') if iname != '-' 
          else os.fdopen(sys.stdout.fileno(),'wb')) as f_out, \
         (open(iname,'rb') if iname != '-' 
          else os.fdopen(sys.stdin.fileno(),'rb')) as f_in:

        while not(last) or not(qu.empty()):

            while not(last) and qu.qsize() < Qmax:
                c = f_in.read(chunk)
                if type(c) is bytes and len(c) > 0:
                    t = tempfile.NamedTemporaryFile(prefix='%06x-'%i,suffix='-%d'%pid,dir=tmp_dir)
                    t.write( c )
                    if dg:
                        m=hashlib.md5()
                        m.update(c)
                        dig[i] = m.hexdigest()
                    t.flush()
                    z = GZipper(t) 
                    if verbosity > 0: print('spawn:\t',z,file=sys.stderr)
                    z.put_i = i
                    i += 1
                    qu.put(z)
                    z.start()
                if len(c) < chunk:
                    last = True
                    break
            z = None

            if not qu.empty():
                q = qu.get_nowait()
                q.join()
                if verbosity > 0: print('reap:\t',q,file=sys.stderr)
                f_out.write(open(q.n_out,'rb').read())
                get_i = q.put_i
                if dg: dg.write('{}\t{}\t{}\n'.format(get_i, dig[get_i], os.stat(q.n_out).st_size))
                if not keep: os.unlink(q.n_out)
            q = None

