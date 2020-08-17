#!/usr/bin/env python2

import tempfile
import multiprocessing
import sys
import os
import time
import gzip
import hashlib
import getopt
import Queue
import signal

pid = os.getpid()
print (pid,)

intr = None
def sig(*x):
   global intr
   intr = x[:1]


class G(multiprocessing.Process):
    def __init__(self,n):
        super(G,self).__init__()
        self.n = n
    def run(self):
        signal.signal(signal.SIGINT, sig)
        signal.signal(signal.SIGTERM, sig)
        e = None
        try: time.sleep(self.n)
        except Exception as e:
          pass
        else: e = 1
        finally: 
          print 'in subproc, exc =  ',e
          print '            intr = ',intr

g = G(10)
g.start()
signal.signal(signal.SIGTERM, lambda *x: g.terminate()) # for parent
try:
  time.sleep(10)
except:
  if g.is_alive(): g.terminate()
  print "main - exc"
