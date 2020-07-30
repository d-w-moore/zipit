#!/usr/bin/env python

from __future__ import print_function
import sys,threading,hashlib,time
import gzip,io,getopt
import pprint,operator
import multiprocessing

chunk = 65536

def run(buf,qu=None):
  f = io.BytesIO(buf)
  g = gzip.GzipFile(fileobj=f)
  m = hashlib.md5()
  s = '.'
  while s:
    s = g.read(chunk)
    m.update(s)
  if qu: qu.put(m.hexdigest())
  
def Checker (buf):
  qu = multiprocessing.Queue()
  return qu, multiprocessing.Process(target=run,args=(buf,qu))

thr={}
q=4
verbose=False

def usage():
  print('''
  Usage: {0} [Options...] {{compressed_file}} {{digest_file}}
     or: {0} [Options...] {{compressed_file}} {{< digest_file}}
  Options:
           -h      print this help
           -v      verbosely print successful md5 matches
           -q N    use q CPU cores at a time
  
           '''.format(sys.argv[0],**globals()))
  exit()

opt,arg=getopt.getopt(sys.argv[1:],'vhq:')
for k,v in opt:
  if   k == '-h': usage()
  if   k == '-q': q = int(v)
  elif k == '-v': verbose=True

if not arg[:1]:
  usage()

fn=''
offset=0
idx={}
zin=open(arg[0],'rb')
infil = sys.stdin
if arg[1:]: 
  fn = arg[1]
  infil = open(fn)

for line in infil:
    n,dig,sz = line.split('\t')
    size=int(sz)
    idx[int(n)]=(dig,size,offset)
    offset += size

n = 0
for i in range(q):
    if i in idx:
      n = i
      thr[n] = c = Checker(zin.read(idx[n][1]))
      c[1].start()
      n += 1
    else: break

results = {}

while thr :
    time.sleep(0.1)
    rj = [ (i,t) for i,t in thr.items() if not t[1].is_alive() ]
    for i,done in rj:
      done[1].join()
      check = (done[0].get() == idx[i][0])
      results[i] = check
      if verbose: print('done: ',i,check,file=sys.stderr)
      del thr[i]
    while len(thr) < q and n in idx:
      thr[n] = c = Checker(zin.read(idx[n][1]))
      c[1].start()
      n += 1

success_code = 0 if not [i for i in results.values() if not i] \
               else 2 

print("Integrity check {} on".format ("succeeded" if success_code == 0 else "failed"),
       ("'{}'".format(fn) if fn else "stdin"),
       file=sys.stderr
)

exit(success_code)
