from intreader import IntReader
from intwriter import IntWriter
from utils import *
import datetime
import sys



class IntConvert:
    def __init__(self, intr, intw, intcp, inth=None):
        """

        :param intr: reader stream handle
        :param intw: writer stream handle
        :param intformat: conversion logic  (CSV->DATABASE (0), CSV->JSON (1))
        """
        self.ts = datetime.datetime.today()
        self.checkpoint = {'from': intcp, 'to': self.ts}
        self.intr = intr
        self.intw = intw
        self.intr['checkpoint'] = self.checkpoint
        self.intformat = intw['type']
        self.header = inth
        return

    def convert(self):
        """
        Row-by-row conversion to pass structured data further
        1. Read the first string separately if required or if no meta-data exists
        2. ..

        """
        try:
            rowr = IntReader(self.intr)
            roww = IntWriter(self.intw)
            stread = 0
            stconverted = 0
            for row in rowr.read():
                stconverted += roww.append(self._format(row))
                stread += 1
            roww.close()
            print("Processed " + str(stconverted) + " rows for data source .....")
        except:
            print("Unexpected error (converting):", sys.exc_info())
            roww.close()
            return {'Read': stread, 'Written': roww.written(), 'Converted': stconverted, 'Status': 4}
        return {'Read': stread, 'Written': roww.written(), 'Converted': stconverted, 'Status': 8}

    def _format(self, r):
        """serialize read strings to json"""
        w = dict(zip(self.header, r))
        w['inttimestamp'] = self.ts
        return w
