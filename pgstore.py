#!/usr/bin/python2.6
# encoding: utf-8
import collectd
import psycopg2
import threading
from time import sleep
import logging


##############
## settings ##
##############
# how many threads start at boot
start_threads = 5
# max count of threads
max_threads = 50
# min count of threads in idle
min_threads = 2
# spawn new thread for every rows_per_thread line in queue
rows_per_thread = 50
# dbconfig
user = 'collectd'
dbname = 'collectd'
password = 'Secret'
host = 'localhost'
## config ends ##

queue = []
handler = None
rows = 0

# logger
f = logging.Formatter("%(levelname)s %(asctime)s %(lineno)d %(funcName)s: %(message)s")
x = logging.getLogger("log")
x.setLevel(logging.WARNING)
h = logging.FileHandler("/var/log/pgstore.log")
# or stdout
#h = logging.StreamHandler()
h.setFormatter(f)
x.addHandler(h)

def resetrows():
    global rows 
    rows = 0

class Worker(threading.Thread):
    def __init__(self, queue, logging, num):
        threading.Thread.__init__(self)
        self.queue = queue
        self.num = num
        self.kill = False
        self.log = logging

        self.log.critical("Thread %s connecting to database %s at server %s using username %s" % (self.num, dbname, host, user))
        try:
            self.conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'" % (dbname, user, host, password))
            self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            self.cur = self.conn.cursor()
        except:
            self.log.critical("Can't contact database %s at server %s using username %s" % (dbname, host, user))
            sleep(10)
            return False

        self.log.info("Successfully connected to database %s@%s" % (dbname, host))
        try:
            # try to create table if not exists
            self.cur.execute("""
                CREATE TABLE collectd_data (id bigserial PRIMARY KEY,
                    type            varchar,
                    type_instance	varchar,
                    plugin          varchar,
                    plugin_instance	varchar,
                    host            varchar,
                    time            timestamptz,
                    values          float[]
                ) 
            """)

            self.cur.execute("""CREATE INDEX collectd_data_host_idx ON collectd_data (host)""")
            self.cur.execute("""CREATE INDEX collectd_data_time_idx ON collectd_data (time)""")
            self.cur.execute("""CREATE INDEX collectd_data_plugin_idx ON collectd_data (plugin)""")
            self.cur.execute("""CREATE INDEX collectd_data_type_idx ON collectd_data (type)""")
        except psycopg2.ProgrammingError:
            pass
        except Exception as error:
            self.log.critical("Error while executing initial commands on database")
            self.log.critical("%s" % type(error))
            self.log.critical("%s" % error.message)
            return
        

    def stop(self):
        self.kill = True

    def run(self):
        self.log.info("Thread %s started" % self.num)
        while self.kill is False:
            try:
                data = self.queue.pop()
                self.cur.execute(
                    "INSERT INTO collectd_data (type,type_instance,plugin,plugin_instance,host,time,values) VALUES (%s,%s,%s,%s,%s,%s,%s);",
                    (data.type,data.type_instance,data.plugin,data.plugin_instance,data.host,psycopg2.TimestampFromTicks(data.time),data.values)
                )
            except IndexError:
                sleep(0.1)
                pass
            except Exception as error:
                self.log.critical("Got error while writing to database")
                self.log.critical("%s" % type(error))
                self.log.critical("%s" % error.message)
                self.kill = True

        self.conn.close()
        self.log.debug("Successfully disconnected from database")
        self.log.info("Thread %s stopped" % self.num)

class Handler(threading.Thread):
    def __init__(self,queue):
        threading.Thread.__init__(self)
        self.threads = []
        self.queue = queue
        self.threadnum = 0
        self.killall = False
        self.log = logging.getLogger("log")

    def threadcount(self):
        return int(len(self.threads))

    def spawn(self):
        thread = Worker(self.queue, logging.getLogger("log"), self.threadnum)
        thread.start()
        self.threads.append(thread)
        self.threadnum += 1

    def stopall(self):
        self.log.info("stopping threads")
        for i in self.threads:
            i.stop()
        self.log.info("threads stopped")

    def stop(self):
        self.killall = True

    def run(self):
        try:
            self.log.info("Starting %s threads" % start_threads)
            for i in range(0,start_threads):
                self.spawn()

            while self.killall is False:
                self.log.debug("I have %s threads" % len(self.threads))
                self.log.debug("Queue is %s long" % len(self.queue))
                q = len(self.queue) / rows_per_thread
                if len(self.threads) < q:
                    a =  q - len(self.threads)
                    if ( a + len(self.threads)) > max_threads:
                        a = max_threads -  len(self.threads)
                    self.log.debug("%s new threads needed" % a)
                    for i in range(0,a):
                        self.spawn()
                elif len(self.threads) > q:
                    a = len(self.threads) - q
                    if ( len(self.threads) - a) < min_threads:
                        a = len(self.threads) - min_threads
                    for i in range(0,a):
                        t = self.threads.pop(1)
                        t.stop()
                restart = []
                for thread in range(0,len(self.threads)):
                    if self.threads[thread].isAlive() is not True:
                        restart.append(thread)

                if len(restart) is not 0:
                    self.log.debug("Restarting %s threads" % len(restart))

                for i in restart:
                    t = self.threads.pop(i)
                    self.log.warning("Thread %s is dead, restarting" % t.name)
                    thread = Worker(queue, logging.getLogger("log"), self.threadnum)
                    thread.start()
                    self.threads.append(thread)
                sleep(1)
            self.stopall()

        except Exception as error:
            self.log.critical("Got unexcepted error: %s" % type(error))
            self.log.critical("%s" % error)
            self.stopall()

def init():
    global handler
    handler = Handler(queue)
    handler.start()

def uninit():
    global handler
    handler.stop()

def write(data):
    global rows
    queue.append(data)
    rows += 1

def read(data=None):
    # stats about me :)
    global rows
    global handler
    v1 = collectd.Values(type='gauge', interval=10)
    v1.plugin='pgstore-rows'
    v1.dispatch(values=[rows / 10])
    resetrows()
    v2 = collectd.Values(type='gauge', interval=10)
    v2.plugin='pgstore-threads'
    v2.dispatch(values=[handler.threadcount()])

collectd.register_write(write)
collectd.register_read(read)
collectd.register_init(init)
collectd.register_shutdown(uninit)
