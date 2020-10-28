#Tristan Jones
#PS:1486089
#Hw3

#python3 transaction-bookings.py "input=trans.txt;transaction=y;threads=10"

import psycopg2
import sys
import threading
import random
from random import randrange
import time
import string
from re import search
import re
import queue

# For use in signaling
shutdown_event = threading.Event()

# Thread def
class worker(threading.Thread):

    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.work=queue

    def run(self):

        for item in iter(self.work.get, None): # This will call self.work.get() until None is returned, at which point the loop will break.
            #Open connection
            with open('password.txt') as f:
                lines = [line.rstrip() for line in f]
                
            username = lines[0]
            pg_password = lines[1]

            conn = psycopg2.connect(database = "COSC3380", user = username, password = pg_password)
            conn.autocommit = True
            cursor = conn.cursor()

            #Split line from queue
            x = item.split(',')
            pId = (x[0])
            fId = (x[1])
            
            print("pId:", pId)
            print("fId:", fId)

            #Inserting
            cursor.execute("INSERT INTO bookings \
                            VALUES ('{}',current_timestamp, '300') \
                            RETURNING book_ref;".format(randrange(100000,999999)))
            r = str(cursor.fetchall()[0][0])
            print(r)


            #Updating



            #Queue Done
            self.work.task_done()
        self.work.task_done()


def main():


    # Seperating command line
    argv=sys.argv[1]

    equalPos = [m.start() for m in re.finditer('=', argv)]
    colonPos = [m.start() for m in re.finditer(';', argv)]

    txt=argv[equalPos[0] + 1:colonPos[0]]
    transaction=argv[equalPos[1] + 1:colonPos[1]]
    threadsNum=argv[equalPos[2] + 1:]
    threadsNum = int(threadsNum)
    
    """ print("\n")
    print(argv)
    print("Transaction =",transaction)
    print("Threads =",threadsNum)
    print("\n") """

    # Seperating the lines
    with open(txt) as f:
        next(f)
        lines = [line.rstrip() for line in f]


    # Inserting lines into queue for threads
    while("" in lines) : 
        lines.remove("") 

    work = queue.Queue()
    

    # Hold threads
    threads = []
    threadId = 1

    # Loop/create/start threads
    for x in range(threadsNum):
        t = worker(work)
        t.setDaemon(True)
        t.start()
        threads.append(t)
        threadId += 1

    print("Waiting for threads to complete...")



    for line in lines:
        work.put(line)

    for i in range(threadsNum):  # Shut down all the workers
        work.put(None)


    work.join()





"""     # Join Threads and Queue, Clean up
    try:
        for i in threads:
            i.join(timeout=1.0)
    except (KeyboardInterrupt, SystemExit):
        print("Caught Ctrl-C. Cleaning up. Exiting.")
        shutdown_event.set() 
        
"""

main()
