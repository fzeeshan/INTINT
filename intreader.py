import os
import csv
import pypyodbc
import datetime

class IntReader:
    def __init__(self, source):
        self.intconnstr = source
        self.inttype = source['type']  #database, file, etc.
        self.intconnstr = source['connstr']  #connection string: Server,Port,db/filename
        self.intmethod = source['method']  #append method description (api, rest, query, etc.)
        self.intname = source['name']  #name of table or procedure or whatever else
        self.connect()

    def connect(self):
        return

    def __iter__(self):
        return self

    def __next__(self):
        return 1
