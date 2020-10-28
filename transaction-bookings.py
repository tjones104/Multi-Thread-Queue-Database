#Tristan Jones
#PS:1486089
#Hw3

#python3 transaction-bookings.py "input=trans.txt;transaction=y;threads=10"

import psycopg2
import sys
import threading
import random
import time
import string
from re import search
import re
import queue

# For use in signaling
shutdown_event = threading.Event()


def ins(threadid=1):
    

    with open('password.txt') as f:
        lines = [line.rstrip() for line in f]
        
    username = lines[0]
    pg_password = lines[1]

    conn = psycopg2.connect(database = "COSC3380", user = username, password = pg_password)
    conn.autocommit = True
    cursor = conn.cursor()



    cursor.execute("INSERT INTO bookings \
                    VALUES ('{}',current_timestamp, '2') \
                    RETURNING book_date;".format(threadid))
    r = str(cursor.fetchall()[0][0])
    print(r)


def main():


    #Seperating command line
    argv=sys.argv[1]

    equalPos = [m.start() for m in re.finditer('=', argv)]
    colonPos = [m.start() for m in re.finditer(';', argv)]

    txt=argv[equalPos[0] + 1:colonPos[0]]
    transaction=argv[equalPos[1] + 1:colonPos[1]]
    threads=argv[equalPos[2] + 1:]
    
    print("\n")
    print(argv)

    

    print("Transaction =",transaction)
    print("Threads =",threads)

    print("\n")


    with open(txt) as f:
        next(f)
        lines = [line.rstrip() for line in f]



    while("" in lines) : 
        lines.remove("") 

    work = queue.Queue()

    for line in lines:
        work.put(line)


    x = work.get().split(',')
    pId = (x[0])
    fId = (x[1])
    
    print(pId)
    print(fId)


    x = work.get().split(',')
    pId = (x[0])
    fId = (x[1])

    """ for line in lines:
            x = line.split(',')
            pId.append(x[0])
            fId.append(x[1]) """

    print(pId)
    print(fId)

    print("\n")

    # Hold threads
    threads = []
    threadId = 1

    # Loop/create/start threads
    for x in range(10):
        t = threading.Thread(target=ins, args=(threadId,))
        t.start()
        threads.append(t)
        threadId += 1

    print("Waiting for threads to complete...")

    try:
        for i in threads:
            i.join(timeout=1.0)
    except (KeyboardInterrupt, SystemExit):
        print("Caught Ctrl-C. Cleaning up. Exiting.")
        shutdown_event.set()





    

main()
