import os
import json
import pyodbc
import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool
import datetime
from concurrent.futures import ThreadPoolExecutor, wait
import multiprocessing
import sys
import hashlib
from utils import *

THREADNUM = 16

class IntWriter:
    def __init__(self, target):
        self.inttype = target['type']  #database, file, etc.
        self.intconnstr = target['connstr']  #connection string: Server,Port,db/filename
        self.mdmconnstr = 'Driver={ODBC Driver 13 for SQL Server}; Server=localhost; Database=MDM_PROD; UID=int_etl; PWD=ugpassword;'
        self.mdmquery = 'SELECT [ID],[UID] FROM [MDM_PROD].[MODEL].[OBJECTS] where SystemID = ? and deletiondate is null'
        self.goldenquery = 'SELECT [XID] as [ID],[UniqueObjectID] as [GoldenID] FROM [MDM_PROD].[MODEL].[mv_xref] where SystemID = ? and [UniqueObjectID] is not null'
        self.mdmssys = target['ssys']  #source system code for UID lookup in MDM
        self.intencoding = target['encoding']  #append method description (api, rest, query, etc.)
        self.intname = target['name']  #name of table or procedure or whatever else
        self.lookupcolumns = target['lookupcolumns']
        self.pool = None
        self.conn = None
        self.curr = None
        self.wcounter = 0
        self.stream = []
        self.intheader = target['header']
        self.lookup_table = dict()
        self.golden_table = dict()
        self.ods_to_dwh_table = set()
        self.cache_dict = dict()
        self.convtime = datetime.timedelta()

        self.connect()
        self.active = True
        self.executor = ThreadPoolExecutor(max_workers=THREADNUM)
        self.futures = []

    def golden_tracker(self):
        cursor = pyodbc.connect(self.mdmconnstr).execute(self.goldenquery, (self.mdmssys,))
        for row in cursor:
            self.golden_table[row[0]] = row[1]
        logging.info(len(self.golden_table), 'golden IDs are mapped to datasource. Memory used: ', sys.getsizeof(self.golden_table))

    def ods_to_dwh_tracker(self):
        cursor = pyodbc.connect(self.intconnstr).execute('select odsid from ' + self.intname)
        self.ods_to_dwh_table.update([row[0] for row in cursor])
        logging.info(len(self.ods_to_dwh_table), 'records already in Staging area. Memory used: ', sys.getsizeof(self.ods_to_dwh_table))


    def change_tracker(self, dtype):
        query = "select ddochash, dcontenthash from public.v_fact where dtype = %s"
        db = psycopg2.connect(self.intconnstr)
        cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(query, (dtype,))
        for row in cursor.fetchall():
            self.cache_dict[row['ddochash'].tobytes()] = row['dcontenthash']

    def connect(self):
        t = datetime.datetime.today()
        # TODO: move to a separate function to make program independant of MDM system
        cursor = pyodbc.connect(self.mdmconnstr).execute(self.mdmquery, (self.mdmssys,))
        columns = [column[0] for column in cursor.description]
        for row in cursor.fetchall():
            self.lookup_table[row[1]] = row[0]
        # print(self.lookup_table)
        self.golden_tracker()
        if self.inttype == 'odsf1':
            self.pool = ThreadedConnectionPool(1, THREADNUM + 1, self.intconnstr)
        if self.inttype == 'staf1':
            self.ods_to_dwh_tracker()
        if self.intname == 'KS2':  # TODO: add proper lookup of possible systems or some other logic when to look for changes (may be target system)
            self.change_tracker(self.intname)
            logging.info('Cache initialization took  ' + str(datetime.datetime.today() - t))
        return

    def clear(self):
        self.stream.clear()
        return

    def written(self):
        print(self.wcounter)
        return self.wcounter

    def __len__(self):
        return len(self.stream)

    def append(self, data):
        st = datetime.datetime.now()
        BATCH_SIZE = 1

        if self.inttype == 'apij1':
            BATCH_SIZE = 1000
            objectkeys = ['ExtractionDate','Migration','ActionID','SystemID','EntityID','UID','ParentUID','Verified','UXID','ValidFrom','ValidTo']
            obj = {}

            if 'PeriodObjects' in data:
                obj['ExtractionDate'] = data['ExtractionDate']
                obj['Migration'] = data['Migration']
                obj['ActionID'] = data['ActionID']
                obj['SystemID'] = data['SystemID']
                obj['EntityID'] = data['EntityID']
                obj['UID'] = data['UID']
                obj['ParentUID'] = data['ParentUID']
                obj['Verified'] = data['Verified']
                obj['UXID'] = data['UXID']
                obj['PeriodObjects'] = data['PeriodObjects']
            else:
                obj['PeriodObjects'] = []
                obj['PeriodObjects'].append({'Attributes': []})
                if 'ValidFrom' in data:
                    obj['PeriodObjects'][0]['ValidFrom'] = data['ValidFrom']
                if 'ValidTo' in data:
                    obj['PeriodObjects'][0]['ValidTo'] = data['ValidTo']
                for key in data.keys():
                    if key not in objectkeys:
                        if data[key] in self.lookup_table:
                            data[key] = self.lookup_table[data[key]]
                        obj['PeriodObjects'][0]['Attributes'].append({'Name': key, 'Value': str(data[key]).replace('00000000-0000-0000-0000-000000000000', '#NULL')})
                    else:
                        obj[key] = str(data[key]).replace('00000000-0000-0000-0000-000000000000', '#NULL')
                obj['ActionID'] = 3    # Force-set action as "integration"

        elif self.inttype == 'odsf1':
            objectkeys = ['DataType','SystemID','ActionID','ExtractionDate','DocumentUID','Ln','inttimestamp']
            obj = dict()
            obj['dtimestamp'] = data['inttimestamp']
            obj['dextractiondate'] = data['ExtractionDate']
            obj['dtype'] = data['DataType']
            obj['dsystem'] = data['SystemID']
            obj['ddocuid'] = data['DocumentUID']
            obj['ddocln'] = data['Ln']
            obj['ddochash'] = hashlib.md5((str(obj['ddocuid']) + str(obj['ddocln'])).encode('utf-8')).digest()
            # filter elements where GUID lookup failed --- NO IMPORT before GUIDs are in MDM
            errs = [(k,v) for (k,v) in data.items() if k in self.lookupcolumns and v not in self.lookup_table and v != '00000000-0000-0000-0000-000000000000']
            if len(errs) > 0:
                logging.warning('Failed to convert GUID for %s', str(errs))
                self.convtime += datetime.datetime.now() - st
                return 0
            obj['dcontent'] = json.dumps({k:self.lookup_table[v] if v in self.lookup_table else v.replace('00000000-0000-0000-0000-000000000000', '#NULL')
                                          for (k,v) in data.items() if k not in objectkeys}, sort_keys=True)
            obj['dcontenthash'] = hashlib.md5(obj['dcontent'].encode('utf-8')).digest()
            obj['delta'] = False
            if obj['ddochash'] in self.cache_dict:
                # This line has been already posted so we need to check if the last available record is actual
                # flag line as delta
                obj['delta'] = True
                if self.cache_dict[obj['ddochash']].tobytes() == obj['dcontenthash']:
                    # Can update some field here with a timestamp to guaranteee that data is actual
                    self.convtime += datetime.datetime.now() - st
                    return 0
                # Earlier version exists so we have to create a new record for this version

        elif self.inttype == 'staf1':
            obj = data.copy()
            if obj['odsid'] in self.ods_to_dwh_table:
                self.convtime += datetime.datetime.now() - st
                return 0
            # TODO: this list of fields should be another field in sources table
            golden_entities = ['ProjectUID', 'ConstrObjectUID']
            for key in golden_entities:
                if obj[key] not in self.golden_table:
                    logging.warning('Failed to find golden ID for record %s  %s', str(obj[key]), str(key))
                    self.convtime += datetime.datetime.now() - st
                    return 0
                obj[key] = self.golden_table[obj[key]]
            # treat records which dont need to have golden values - pass nulls to fit into sql requirements
            for key in obj:
                if obj[key] == '#NULL':
                    obj[key] = None
        self.convtime += datetime.datetime.now() - st
        self.stream.append(obj)
        if len(self.stream) == BATCH_SIZE:
            self.futures.append(self.executor.submit(self.commitp, {'ContextRef': '', 'Objects': self.stream.copy()}))
            self.clear()
        return 1

    def close(self):
        if len(self.stream) > 0:
            self.futures.append(self.executor.submit(self.commitp, {'ContextRef': '', 'Objects': self.stream.copy()}))
            self.clear()
        wait(self.futures)
        self.wcounter = sum([f.result() for f in self.futures])
        self.executor.shutdown(wait=True)
        if self.inttype == 'odsf1':
            safeexecute_pgsql(self.pool, 'refresh materialized view mv_fact_lastv', None, self.intconnstr)
            self.pool.closeall()
        print(self.convtime)
        self.active = False

    def commitp(self, params=None):
        t = datetime.datetime.today()
        count = 0
        if self.inttype == 'apij1':
            if params:
                w = params
            db = pyodbc.connect(self.intconnstr)
            cursor = db.cursor()
            cursor.execute('SET TRANSACTION ISOLATION LEVEL SNAPSHOT')
            cursor.commit()
            query = 'DECLARE @ret int' \
                    '  EXEC @ret = ' + self.intname + ' ?, NULL' \
                                                      ' SELECT @ret'
            try:
                count = cursor.execute(query, [str(json.dumps(w)),]).fetchone()[0]
                cursor.commit()
            except:
                logging.error("Unexpected SQL server error, rolling back:", sys.exc_info())
                logging.error("With object:", w)
                cursor.rollback()
        elif self.inttype == 'odsf1':
            if params and 'Objects' in params:
                w = params['Objects']
                conn = self.pool.getconn()
                cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                for obj in w:
                    query = 'INSERT INTO public.fact(dtype, dsystem, ddocuid, ddocln, ddochash, dcontenthash, dcontent, dtimestamp, dextractiondate, delta)' \
                            '    VALUES (%(dtype)s, %(dsystem)s, %(ddocuid)s, %(ddocln)s, %(ddochash)s, %(dcontenthash)s, %(dcontent)s, %(dtimestamp)s, ' \
                            '%(dextractiondate)s, %(delta)s)'
                    try:
                        cur.execute(query, dict(obj))
                        conn.commit()
                        count += 1
                    except:
                        logging.error("Unexpected PostgreSQL server error, rolling back:", sys.exc_info())
                        logging.error("With object:", obj)
                        conn.rollback()
                self.pool.putconn(conn)
        elif self.inttype == 'staf1':
            # TODO: CHECK
            if params:
                w = params['Objects']
            db = pyodbc.connect(self.intconnstr)
            cursor = db.cursor()
            query = 'INSERT INTO ' + self.intname + '(' + ','.join(self.intheader) + ') VALUES(' + ','.join(['?' for _ in self.intheader]) + ')'
            for obj in w:
                try:
                    cursor.execute(query, tuple([obj[key] for key in self.intheader]))
                    cursor.commit()
                    count += 1
                except:
                    logging.error("Unexpected SQL server error, rolling back:", sys.exc_info())
                    logging.error("With query:", query)
                    logging.error("With object:", obj)
                    cursor.rollback()

        print('Commit took ' + str(datetime.datetime.today() - t))
        return count
