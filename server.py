from multiprocessing import Process, Queue, Manager, Array
import time
import socket
import sys
from multiprocessing.reduction import ForkingPickler
import StringIO
import pickle
import os
import signal

VERBOSITY=5 #1=info 2=info+ 3=debug
DELAY=10 #Slow requests processing time

def forking_dumps(obj):
    buf = StringIO.StringIO()
    ForkingPickler(buf).dump(obj)
    return buf.getvalue()

def printv(string, lvl):
    if lvl<=VERBOSITY:
        print string

def serverProcess(q,threadId,status):
    printv('Started thread '+str(threadId),2)
    while True:
        try:
            conn,addr=pickle.loads(q.get())
            status[threadId]=True
            printv('[Thread] Connection from'+str(addr)+'processing',3)
            data = conn.recv(1024)
            if DELAY>0:
                printv('Waiting '+str(DELAY)+'s',3)
                time.sleep(DELAY)
            if not data:
                conn.close()
                printv('No data received from'+str(addr),2)
                continue
            if data.startswith('HELO '):
                text=data[5:].strip()
                conn.sendall('HELO '+text+'\nIP:'+conn.getsockname()[0]+'\nPort:'+sys.argv[1]+'\nStudentID:16313441\n')
            elif data.strip()=='KILL_SERVICE':
                os.kill(os.getppid(),signal.SIGTERM)
            else:
                conn.sendall('Bad Request.')
            conn.shutdown(socket.SHUT_RDWR)
            conn.close()
            printv('[Thread] Processed request from'+str(addr),3)
        except KeyboardInterrupt:
            printv('Thread '+str(threadId)+' stopped',3)
            sys.exit(0)
        except socket.error:
            printv('Socket error',1)
        except Exception as e:
            printv('Thread failure',1)
            printv(e,2)
            sys.exit(0)
        finally:
            status[threadId]=False

def terminationHandler(_signo, _stack_frame):
    printv('Received KILL_SERVICE',1)
    global threadPool
    for thr in threadPool:
        thr.terminate()

if __name__ == '__main__':
    if len(sys.argv)<3:
        print 'Usage : python2 server.py port numThreads'
        sys.exit(0)
    s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    maxThreadNum=int(sys.argv[2])
    minThreadNum=max(1,int(sys.argv[2])/2)
    try :  #Server setup
        global threadPool
        q=Queue()
	manager=Manager()
        threadStatus=manager.list()
        threadPool=[]
	for pstart in range(int(sys.argv[2])):
            threadStatus.append(False)
            p=Process(target=serverProcess, args=(q,pstart,threadStatus,))
            p.start()
            threadPool.append(p)
        printv('All threads started',1)
        signal.signal(signal.SIGTERM,terminationHandler)
        HOST='0.0.0.0'
        PORT=int(sys.argv[1])
        s.bind((HOST,PORT))
        s.listen(1)
        s.settimeout(10)
        printv('Server started',1)
    except Exception as e:
        print 'Could not start server.'
        printv(e,2)
        for thr in threadPool:
            thr.terminate()
        sys.exit(0)
            
    while True:  #Master loop
        try:
            conn, addr = s.accept()
            printv('[Master] New connection from'+str(addr),3)
            if q.empty() and len(threadPool)>minThreadNum:
                if threadStatus[-1]==False:
                    threadPool[-1].terminate()
                    threadPool.pop()
                    threadStatus.pop()
                    printv('Killed thread. New thread count : '+str(len(threadPool)),2)
            if q.qsize()>2 and len(threadPool)<maxThreadNum:
                threadStatus.append(False)
                p=Process(target=serverProcess, args=(q,len(threadPool),threadStatus,))
                threadPool.append(p)
                p.start()
                printv('Started thread. New thread count : '+str(len(threadPool)),2)
            q.put(forking_dumps([conn,addr]))
            conn.close()
        except KeyboardInterrupt:
            printv('Server stopped from keyboard',1)
            sys.exit(0)
        except socket.timeout:
            pass
