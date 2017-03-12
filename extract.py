import csv
import os
import pypyodbc
import datetime

class Extractor:
    def __init__(self, foldername=None):
        self.filename = ''
        self.tablename = ''
        self.connStr = (
            r'Driver={ODBC Driver 13 for SQL Server};'
            r'Server=localhost;'
            r'Database=DM_STAGING;'
            r'UID=int_etl;'
            r'PWD=ugpassword;'
        )
        self.db = pypyodbc.connect(self.connStr)
        self.cursor = self.db.cursor()

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
