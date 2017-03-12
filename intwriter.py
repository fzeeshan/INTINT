import os
import pypyodbc
import datetime


class IntWriter:
    def __init__(self, target):
        self.inttype = target['type']  #database, file, etc.
        self.intconnstr = target['connstr']  #connection string: Server,Port,db/filename
        self.intmethod = target['method']  #append method description (api, rest, query, etc.)
        self.intname = target['name']  #name of table or procedure or whatever else
        self.connect()

    def connect(self):
        return

    def append(self, data):
        print(data)
        return
