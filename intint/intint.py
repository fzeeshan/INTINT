import json
import time
import asyncio
from multiprocessing import Process, Queue
from threading import Thread
from collections import deque
import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool
import datetime
from intconvert import IntConvert
from intserver import IntServer
from utils import *
import logging
import signal


MODE = 0
SLEEPINTERVAL = 1  # this defines how often queue is checked while idle
UPDATEINTERVAL = 5  # this defines how often queue is checked while idle

# Termination handler
class GracefulKiller:
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self,signum, frame):
        self.kill_now = True


class IntInt(Process):
    def __init__(self, r, killer, mode=0):
        """

        :param mode: 1 for single run through all sources, 0 for run planning through schedule

        """
        with open('config.json') as config_file:
            config = json.load(config_file)
            self.connStr = config['db']['connection_string']
        self.db = None  # Initialize a postgresql connection pool
        # self.cursor = None  # Initialize shared cursor
        self.starttime = None  # Initialize uptime counter variable
        self.killer = killer
        self.intqueue = deque()  # queue is a list of sources represented by dictionaries like: Id,Sort,Source,Target,SourceFormat,TargetFormat,Status,Size
        self.r = r  # shared queue between web-server and scheduler
        self.q = Queue()
        super(IntInt, self).__init__()

    def run(self):
        self.starttime = datetime.datetime.now()
        logging.info('Building queue...')

        self.db = ThreadedConnectionPool(1, 10, self.connStr)  # TODO: move connection params to config
        schedthread = Thread(target=self.readschedule, args=(self.q,))
        schedthread.daemon = True
        schedthread.start()
        # self.cursor = self.db.cursor(cursor_factory=psycopg2.extras.DictCursor)

        while not self.killer.kill_now:
            if not self.r.empty():
                self.fromapi(self.r.get())
            elif not self.q.empty():
                self.fromqueue(self.q.get())
            else:
                time.sleep(SLEEPINTERVAL)
                continue

        # Clean up before shutting down
        print('Shutting down. Uptime:', datetime.datetime.now() - self.starttime)
        self.db.closeall()
        return

    def fromapi(self, params):
        if 'mode' in dict(params).keys():
            if params['mode'] == 'sourceid':
                self.readsources(m=1, sourceid=params['sourceid'])
        return

    def readmeta(self, source):
        """Read metadata definition for format conversion for source->target"""
        # TODO: This piece of code is dependant on database model and should probably be refactored along with the model
        context = {}
        context['source'] = {}
        context['target'] = {}
        context['header'] = source['theader']
        context['source']['header'] = source['sheader']
        context['source']['type'] = source['stype']
        context['source']['connstr'] = source['sconnstr']
        context['source']['encoding'] = source['sencoding']
        context['source']['delimiter'] = source['sdelimiter']
        context['source']['quotechar'] = source['squotechar']
        context['source']['name'] = source['sname']
        context['source']['skiprow'] = source['sskiprow']
        context['target']['header'] = source['theader']
        context['target']['lookupcolumns'] = source['lookupcolumns']
        context['target']['type'] = source['ttype']
        context['target']['connstr'] = source['tconnstr']
        context['target']['encoding'] = source['tencoding']
        context['target']['delimiter'] = source['tdelimiter']
        context['target']['quotechar'] = source['tquotechar']
        context['target']['name'] = source['tname']
        context['target']['ssys'] = source['ssys']
        context['checkpoint'] = source['checkpoint']

        return context

    def updatesource(self, stats):
        """Update source definition with new statistics"""
        query = "INSERT INTO public.runs(sourceid, scheduleid, rowsread, rowsconverted, rowswritten,timeelapsed, rundate, runstatus) " \
                "VALUES(%(SourceID)s, %(ScheduleID)s, %(Read)s, %(Converted)s, %(Written)s, %(Elapsed)s, %(RunStart)s,  %(Status)s)"
        cur = safeexecute_pgsql(self.db, query, stats, self.connStr)
        self.db.putconn(cur.connection)
        return

    def readsources(self, m=0, sourceid=None):
        """Run through possible data sources and check if new data exists"""
        if m == 0:
            query = "select * from public.sources order by sort"
        elif m == 1:
            # TODO: modify query to only select sources by ID
            query = "select * from public.sources where id = %s order by sort"
        cur = safeexecute_pgsql(self.db, query, (sourceid,), self.connStr)
        # TODO: prepare connection string
        if cur.rowcount > 0:
            for source in cur:
                if m == 1:
                    source['scheduleid'] = -1
                self.pushtoqueue(dict(source))
        self.db.putconn(cur.connection)
        return -1

    def readschedule(self, q):
        """Run through possible data sources and check if new data exists"""
        while 1 == 1:
            currenttime = datetime.datetime.today()
            query = "select src.*, sched.id as scheduleid from public.sources as src inner join public.schedules as sched on " \
                    "src.scheduleid = sched.id where src.scheduleid is not null and sched.release = True " \
                    "order by src.sort"
            schedcursor = safeexecute_pgsql(self.db, query, None, self.connStr)
            # TODO: prepare connection string
            if schedcursor.rowcount > 0:
                logging.info(str(datetime.datetime.today()) + '  Released ' + str(schedcursor.rowcount) + ' job(s)...')
                for source in schedcursor:
                    q.put(source)
            self.db.putconn(schedcursor.connection)

            logging.info(str(datetime.datetime.today()) + '  @ Scheduling...')
            # UPDATE schedules clear all release flags
            query = "UPDATE public.schedules set release = False, lastrun = %s where release = True"
            schedcursor = safeexecute_pgsql(self.db, query, (currenttime,), self.connStr)
            # Read schedules
            query = "select * from public.schedules sched"
            self.db.putconn(schedcursor.connection)

            schedules_to_release = list()
            schedcursor = safeexecute_pgsql(self.db, query, None, self.connStr)
            if schedcursor.rowcount > 0:
                for source in schedcursor:
                    if self.parsefrequency(source['starttime'], source['endtime'], source['regularity'], source['lastrun'], currenttime):
                        schedules_to_release.append(source['id'])
            # Set Release flag
            self.db.putconn(schedcursor.connection)

            if len(schedules_to_release) > 0:
                query = "UPDATE public.schedules set release = True where id in %s"
                schedcursor = safeexecute_pgsql(self.db, query, (tuple(schedules_to_release),), self.connStr)
                self.db.putconn(schedcursor.connection)
            # TODO: adjust update interval
            adjsleep = datetime.datetime.today() - currenttime
            time.sleep(UPDATEINTERVAL - adjsleep.total_seconds() if UPDATEINTERVAL > adjsleep.total_seconds() else 0)
        return

    def parsefrequency(self, starttime, endtime, regularity, lastrun, currenttime):
        """

        :param starttime: at what time scheduled job should be put to execution queue
        :param endtime: when to stop scheduling the task (for repeative)
        :param regularity: main parameter - daily/weekly/monthly, for values see doc
        :param lastrun: last job run (we don't care if it was executed or not and whether it was successful,
        this time is used is just to schedule intraday tasks
        :param currenttime: scheduling run start
        :return:
        """
        schedule = False
        logging.debug('Regularity: ' + str(regularity) + ' freq ' + str(regularity.strip()[0]) + ' Starts at: ' +
                  str(starttime) + ' Not later than: ' + str(endtime) + ' Last run: ' + str(lastrun) +
                  ' Scheduled at: ' + str(currenttime))
        if regularity.strip()[0] == 'm':
            sday = int(regularity.strip()[1:])  # pick day and build target activation time
            if sday <= 31:  # TODO: add check for months less than 31 day, including february
                if UPDATEINTERVAL >= (currenttime.replace(day=sday, hour=int(starttime.hour),
                                                          minute=int(starttime.minute), second=int(starttime.second)) - currenttime).total_seconds() >= 0:
                    schedule = True
        elif regularity.strip()[0] == 'w':
            # TODO: need to add some function to shift to next day if today + interval > 24h
            if str(currenttime.isoweekday()) in list(regularity.strip()[1:]):
                if (currenttime.replace(hour=int(starttime.hour), minute=int(starttime.minute),
                                        second=int(starttime.second)) - currenttime).total_seconds() <= UPDATEINTERVAL:
                    schedule = True
        elif regularity.strip()[0] == 'd':
            if not lastrun or currenttime.day != lastrun.day or currenttime - lastrun > datetime.timedelta(days=1):
                if UPDATEINTERVAL >= (currenttime.replace(hour=int(starttime.hour), minute=int(starttime.minute),
                                                      second=int(starttime.second)) - currenttime).total_seconds() >= 0:
                    schedule = True
        elif regularity.strip()[0] == 'r':
            supdinterval = datetime.timedelta(seconds=UPDATEINTERVAL) + currenttime
            if not endtime:
                endtime = datetime.time(hour=23, minute=59, second=59)
            if not starttime:
                starttime = datetime.time(hour=0, minute=0, second=0)
            if endtime >= datetime.time(hour=supdinterval.hour, minute=supdinterval.minute, second=supdinterval.second) >= starttime:
                sinterval = datetime.timedelta(minutes=int(regularity.strip()[1:]))
                if not lastrun or UPDATEINTERVAL >= (lastrun + sinterval - currenttime).total_seconds():
                    schedule = True

        if schedule:
             logging.info('Schedule flagged for release')
        return schedule

    def pushtoqueue(self, source):
        """Add tuple to queue containing source name, format and order"""
        self.q.put(source)
        return

    def fromqueue(self, source):
        """Pick one step from queue and start conversion"""
        # print(source)

        context = self.readmeta(source)
        qstart = datetime.datetime.today()
        logging.info(str(qstart) + '  @ Executing scheduled job step...')
        if context:
            converter = IntConvert(context['source'], context['target'], context['checkpoint'], context['header'])
            st = converter.convert()
            st['Elapsed'] = (datetime.datetime.today() - qstart).total_seconds()
            st['SourceID'] = source['id']
            st['ScheduleID'] = source['scheduleid']
            st['RunStart'] = qstart.isoformat()
            logging.info(json.dumps(st))
            self.updatesource(st)
            return 1
        self.updatesource(source)
        return -1


def main():
    # add initialization like read configs and set constants
    intQueue = Queue()
    killer = GracefulKiller()
    # Start scheduler in a separate process
    schedproc = IntInt(intQueue, killer, MODE)
    schedproc.start()
    # Start web server to lissten
    serverproc = IntServer(intQueue)
    serverproc.run()

if __name__ == "__main__":
    # execute only if run as a script
    main()
