#!/usr/bin/env python

from __future__ import print_function
import sys
import threading
import hashlib
import time
import gzip
import os
import io
import getopt
import types
import tempfile
import operator
import pprint
import multiprocessing

chunk = 65536 * 128

def run(buf,qu,unz):
  f = io.BytesIO(buf)
  g = gzip.GzipFile(fileobj=f)
  m = hashlib.md5()
  s = '.'
  u = []
  while s:
    s = g.read(chunk)
    if unz: u.append(s)
    m.update(s)
  digout=m.hexdigest()
  output = [digout]
  if unz:
    tf = tempfile.NamedTemporaryFile(mode='wb',delete=False)
    tf.write(b''.join(u))
    output.append(tf.name)
  qu.put(output)

def Checker (buf,unz=False):
  qu = multiprocessing.Queue()
  return qu, multiprocessing.Process(target=run,args=(buf,qu,unz))

INTV = 0.1
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
           -o out  enable parallel unzipping. Name "out" file
           -q N    use q CPU cores at a time

           '''.format(sys.argv[0],**globals()))
  exit()

out_fn = None
outf = None

opt,arg=getopt.getopt(sys.argv[1:],'o:vhq:')
for k,v in opt:
  if   k == '-h': usage()
  if   k == '-o': out_fn = v
  if   k == '-q': q = int(v)
  elif k == '-v': verbose=True

if not arg[:1]:
  usage()


if out_fn is not None:
  outf = os.fdopen( sys.stdout.fileno(), 'wb') if out_fn == '-' \
         else (open(out_fn,"wb") if out_fn else '*')

class DummyWriter:
  def __init__(self): pass
  def write(self,*x): pass

def outfile(ck=True,memo=[]):
  if not memo:
    if not(ck) or isinstance(outf, (str, types.NoneType)): memo[:]=[DummyWriter()]
  return outf if not memo else memo[0]

def inorder(): return (out_fn is not None)

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
      thr[n] = c = Checker(zin.read(idx[n][1]), (outf is not None))
      c[1].start()
      n += 1
    else: break

results = {}
j = 0

while thr :

    if not inorder():              # --> buffer the jobs that have finished
      time.sleep(INTV)
      rj = [ (i,t) for i,t in thr.items() if not t[1].is_alive() ]
    else:
      rj = [ (j,thr.pop(j)) ]  # --> wait in sequence
      j += 1

    for i,done in rj:
      done[1].join()

    ckarr = []

    for i,done in rj:
      qout = done[0].get()
      check = (qout[0] == idx[i][0])
      results[i] = check
      ckarr.append(check)
      if qout[1:]:
        outfile(check).write(open(qout[1],'rb').read())
        os.unlink(qout[1])
      if verbose: print('done: ',i,check,file=sys.stderr)
      if i in thr: del thr[i]

    if False not in ckarr:
      while len(thr) < q and n in idx:
        thr[n] = c = Checker(zin.read(idx[n][1]), (outf is not None))
        c[1].start()
        n += 1

success_code = 0 if not [i for i in results.values() if not i] \
               else 2

print("Integrity check {} on".format ("succeeded" if success_code == 0 else "failed"),
       ("'{}'".format(fn) if fn else "stdin"),
       file=sys.stderr
)

exit(success_code)
