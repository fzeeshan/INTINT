import os
import csv
import json
import psycopg2
import pypyodbc
import datetime

class IntReader:
    def __init__(self, source):
        self.inttype = source['type']  # database, file, etc.
        self.intconnstr = source['connstr']  # connection string: Server,Port,db/filename
        self.intencoding = source['encoding']  # source encoding, default: utf-8-sig
        self.intdelimiter = source['delimiter']  # delimiter for text files, default: ;
        self.intquotechar = source['quotechar']  # string char for text files, default: "
        self.intname = source['name']  # name of table or procedure or whatever else
        self.introw = source['skiprow']  # number of rows to skip
        self.header = source['header']
        self.cpfrom = source['checkpoint']['from']
        self.cpto = source['checkpoint']['to']
        self.stream = None

    def read(self):
        route = {'csvs1': self.csvs_read(),
                 'odsf2': self.odsf_read()}
        a = route[self.inttype]
        return a


    def csvs_read(self):
        with open(self.intconnstr, 'r', encoding=self.intencoding) as csvfile:
            self.stream = csv.reader(csvfile, delimiter=self.intdelimiter, quotechar=self.intquotechar)
            print("Source file OK")
            print("Skipping " + str(self.introw) + " row(s)")
            for ix, row in enumerate(self.stream):
                # print("Row " + str(ix))
                if ix < self.introw:
                    continue  # TODO: add metadata check against header
                yield row

    def odsf_read(self):
        db = psycopg2.connect(self.intconnstr)
        cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # if self.cpfrom is None:
        #     self.cpfrom = datetime.datetime.min
        # query = ' SELECT * FROM FACT WHERE dtimestamp >= %s and dtimestamp < %s and dtype = %s'
        # cursor.execute(query, (self.cpfrom, self.cpto, self.intname))
        query = 'SELECT * FROM FACT a join public.mv_fact_lastv b on a.id=b.id WHERE dtype = %s'
        cursor.execute(query, (self.intname,))
        print('Database request OK')
        print(str(cursor.rowcount) + " records read")
        for row in cursor:
            # print("Row " + str(ix))
            dictrow = row.copy()
            listrow = list()
            for key in row['dcontent'].keys():
                dictrow[key] = row['dcontent'][key]
            if 'daction' not in row:
                dictrow['daction'] = 0
            for i in self.header:
                listrow.append(dictrow[i])
            yield listrow
