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

opt,arg=getopt.getopt(sys.argv[1:],'vq:')
for k,v in opt:
  if   k == '-q': q = int(v)
  elif k == '-v': verbose=True

if not arg[:1]:
  print('Error: must supply name of a zipped file as argument 0',file=sys.stderr)
  exit(1)

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
    if idx.has_key(i):
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
    while len(thr) < q and idx.has_key(n):
      thr[n] = c = Checker(zin.read(idx[n][1]))
      c[1].start()
      n += 1

#print ('--results--')
#pprint.pprint( results )

#success if no results are False
exit( 0 if not [i for i in results.values() if not i] else 2 )
