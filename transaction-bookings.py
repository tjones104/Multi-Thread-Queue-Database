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
nSuccessful = 0
nUnsuccessful = 0
nTicketUpdated = 0
nTicketFlightsUpdates = 0
nBookingsUpdated = 0
nFlightsUpdated = 0
nFailed = 0
    

            
# Thread def
class worker(threading.Thread):
    
    def __init__(self, queue, t_state):
        threading.Thread.__init__(self)
        self.work=queue
        self.t_state=t_state

    def run(self):
        

        for item in iter(self.work.get, None): # This will call self.work.get() until None is returned, at which point the loop will break.
            #Open connection
            with open('password.txt') as f:
                lines = [line.rstrip() for line in f]
                
            username = lines[0]
            pg_password = lines[1]

            conn = psycopg2.connect(database = "COSC3380", user = username, password = pg_password)
            if(self.t_state == 'n'):
                conn.autocommit = True
            else:
                conn.autocommit = False
            cursor = conn.cursor()
            
            #Split line from queue
            x = item.split(',')
            pId = (x[0])
            fId = (x[1])
            
            
            if(self.t_state == 'y'):
                cursor.execute("START TRANSACTION;")

            cursor.execute("SELECT COUNT(1) \
                            FROM flights \
                            WHERE flight_id = {};".format(fId))
            r = str(cursor.fetchall()[0][0])
            if(int(r) == 1):

                tempBook_ref = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
                tempTicket_no = randrange(1000000000000,9999999999999)
                
                #Inserting
                cursor.execute("INSERT INTO bookings \
                                VALUES ('{}',current_timestamp, '300') \
                                RETURNING book_ref;".format(tempBook_ref))
                r = str(cursor.fetchall()[0][0])
                global nBookingsUpdated
                nBookingsUpdated += 1
                #print(r)
                
                #Checking for valid flight
                
                cursor.execute("UPDATE flights \
                                SET seats_available = seats_available - 1 \
                                WHERE flight_id = '{}' RETURNING seats_available".format(fId))
                r = str(cursor.fetchall()[0][0])
                
                if (int(r) < 0):
                    #print("No more seats")
                    cursor.execute("UPDATE flights \
                                    SET seats_available = seats_available + 1 \
                                    WHERE flight_id = '{}'".format(fId))
                    cursor.execute("UPDATE bookings \
                                    SET total_amount = 0 \
                                    WHERE book_ref = '{}';".format(tempBook_ref))
                    global nUnsuccessful
                    nUnsuccessful += 1
                    if(self.t_state == 'y'):
                        cursor.execute("COMMIT;")

                else:
                    #print("{} Seats Available".format(r))
                    cursor.execute("INSERT INTO ticket \
                                    VALUES ({},'{}',{},' ',' ',' ') \
                                    RETURNING ticket_no;".format(tempTicket_no,tempBook_ref,pId))
                    r = str(cursor.fetchall()[0][0])
                    #print(r)
                    global nTicketUpdated
                    nTicketUpdated += 1

                    cursor.execute("INSERT INTO ticket_flights \
                                    VALUES ({},{},'Economy','300') \
                                    RETURNING ticket_no;".format(tempTicket_no,fId))
                    r = str(cursor.fetchall()[0][0])
                    #print(r)
                    global nTicketFlightsUpdates
                    nTicketFlightsUpdates += 1

                    updateBooked = "UPDATE flights \
                                    SET seats_booked = seats_booked + 1 \
                                    WHERE flight_id = '{}'".format(fId)
                    cursor.execute(updateBooked)
                    global nFlightsUpdated
                    nFlightsUpdated += 1


                    global nSuccessful
                    nSuccessful += 1
                    if(self.t_state == 'y'):
                        cursor.execute("COMMIT;")



                #Queue Done
                self.work.task_done()
            else:
                global nFailed
                nFailed += 1
            if(self.t_state == 'y'):
                conn.commit()
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
        t = worker(work, transaction)
        t.setDaemon(True)
        t.start()
        threads.append(t)
        threadId += 1

   

    #print("Got here 1")
    for line in lines:
        work.put(line)
    #print("Got here 2")
    for i in range(threadsNum):  # Shut down all the workers
        work.put(None)

    #print("Got here 3")
    # Join Threads and Queue, Clean up
    try:
        for i in threads:
            i.join(timeout=1.0)
    except (KeyboardInterrupt, SystemExit):
        print("Caught Ctrl-C. Cleaning up. Exiting.")
        shutdown_event.set() 
    #print("Got here 4")

    
    nUnsuccessful = nBookingsUpdated - nTicketUpdated

    print("Failed transactions: " + str(nFailed))
    print("Successful transactions: " + str(nSuccessful))
    print("Unsuccessful transactions: " + str(nUnsuccessful))
    print("Records updated for table ticket: " + str(nTicketUpdated))
    print("Records updated for table ticket flights: " + str(nTicketFlightsUpdates))
    print("Records updated for table bookings: " + str(nBookingsUpdated))
    print("Records updated for table flights: " + str(nFlightsUpdated))



   
    
        


main()
