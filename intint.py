import os
import pypyodbc
import datetime
from intreader import IntReader
from intwriter import IntWriter


class IntInt:
    def __init__(self, foldername=None):
        self.configfilename = ''
        self.configtablename = ''
        self.connStr = (
            r'Driver={ODBC Driver 13 for SQL Server};'
            r'Server=localhost;'
            r'Database=DM_STAGING;'
            r'UID=int_etl;'
            r'PWD=ugpassword;'
        )
        self.db = pypyodbc.connect(self.connStr)
        self.cursor = self.db.cursor()
        self.intqueue = []  # queue is a list of dictionaries like: Id,Sort,Source,Target,SourceFormat,TargetFormat,Status,Size
        self.readsources()
        self.intqueue.reverse()


    def readmeta(self):
        """Read metadata definition for format conversion for source->target"""
        return

    def readsources(self):
        """Run through possible data sources and check if new data exists"""
        return

    def pushtoqueue(self):
        """Add tuple to queue containing source name, format and order"""
        return

    def popfromqueue(self):
        """Pick one step from queue and start conversion"""
        source = self.intqueue.pop()
        context = self.readmeta(source)
        if context:
            converter = IntConvert()
            st = converter.convert()
            return st
        return -1


class IntConvert:
    def __init__(self, intr, intw, intmeta=None, intformat=0):
        """

        :param intr: reader stream handle
        :param intw: writer stream handle
        :param intformat: conversion logic  (CSV->DATABASE (0), CSV->JSON (1))
        """
        self.intr = intr
        self.intw = intw
        self.intformat = intformat
        self.header = intmeta
        return

    def convert(self):
        """
        Row-by-row conversion to pass structured data further
        1. Read the first string separately if required or if no meta-data exists
        2. ..

        """
        rowr = IntReader(self.intr)
        roww = IntWriter(self.intw)
        stprocessed = 0
        for row in rowr:
            roww.append(self._format(row))
            stprocessed += 1
        print("Processed " + str(stprocessed) + "rows for data source .....")
        return stprocessed

    def _format(self, r):
        """Apply format to incoming string"""
        if self.intformat == 1:
            w = dict(zip(self.header, r))
        return w

    def read_e0_table(self, tablename, filename):
        self.filename = filename
        self.tablename = tablename
        print('Reading CSV file...')
        with open(self.filename, 'r', encoding="utf-8-sig") as csvfile:
            extractionreader = csv.reader(csvfile, delimiter=';', quotechar='"')
            for ix, row in enumerate(extractionreader):
                if ix == 0:
                    print('Successfully read file. \nFirst row contents: ' + str(row))
                    self._er_routines(', '.join(['[' + r + ']' for r in row]), row)
                    continue
                elif ix % 10000 == 0:
                    print(str(ix) + ' lines read')
                query = "INSERT INTO " + self.tablename + " VALUES(" + ', '.join(['?' for r in row]) + ")"
                # ADD TRY CATCH FOR PROPER ERROR HANDLING
                self.cursor.execute(query, row)
                self.cursor.commit()
        # add some logic to properly destroy the element

    def _er_routines(self, header, row):
        self.cursor.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND  TABLE_NAME = '" + self.tablename + "'")
        records = self.cursor.fetchone()
        if records:
            print('Truncate existing table ' + self.tablename + '...')
            # add truncation before loading
            query = 'TRUNCATE TABLE dbo.' + self.tablename
            self.cursor.execute(query)
            self.cursor.commit()
        else:
            print('Table ' + self.tablename + ' not found.')
            print('Creating database with header: ' + header)
            # create table here
            query = 'CREATE TABLE [dbo].[' + self.tablename + ']( [ID] [bigint] IDENTITY(1,1) NOT NULL ,' + ', '.join(['[' + r + '][nvarchar](4000) NULL' for r in row]) + ')'
            print(query)
            self.cursor.execute(query)
            self.cursor.commit()


def main():
    # some inputs like filenames and table names before going fully autonomous
    # KS2 fact
    e = Extractor()
    e.read_e0_table('E0_SMR_KS', '\\\\snh.ru\\go\\Exchange\\NSI\\PP\\fks2.csv')

if __name__ == "__main__":
    # execute only if run as a script
    main()
