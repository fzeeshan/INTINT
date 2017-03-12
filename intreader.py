import os
import csv
import pypyodbc
import datetime

class IntReader:
    def __init__(self, source):
        self.intconnstr = source
        self.inttype = source['type']  # database, file, etc.
        self.intconnstr = source['connstr']  # connection string: Server,Port,db/filename
        self.intencoding = source['encoding']  # source encoding
        self.intdelimiter = source['delimiter']  # delimiter for text files
        self.intquotechar = source['quotechar']  # string char for text files
        self.intname = source['name']  # name of table or procedure or whatever else
        self.stream = None
        self.connect()

    def connect(self):
        if self.inttype == 'csv1':
            with open(self.intconnstr, 'r', encoding=self.intencoding) as csvfile:
                self.stream = csv.reader(csvfile, delimiter=';', quotechar='"')
            return
        return

    def __iter__(self):
        return self

    def __next__(self):
        return 1
