import sqlite3
import sys
import logging
import datetime
import random
import csv
from logSettings import createLogger, closeLogging

logger = createLogger(__name__)
logger.addHandler(logging.StreamHandler())

class databaseConnect():


    def __init__(self, dbName):
        try:
            self.conn = sqlite3.connect(dbName,
                                        detect_types = sqlite3.PARSE_DECLTYPES|
                                        sqlite3.PARSE_COLNAMES)
            self.c = self.conn.cursor()
            self.logger = createLogger(__name__)
            self.logger.debug('db connected')
            self.c.execute('''CREATE TABLE IF NOT EXISTS nameTable(
                            name TEXT PRIMARY KEY,
                            active TEXT,
                            prayedFor TEXT,
                            created DATE,
                            last DATE,
                            prayerCount INTEGER)''')
            self.defaultDate = datetime.date(2000, 1, 1) # A not prayed for placeholder

        except Exception:
            self.logger.critical('__init__ error')

    def handleError(self):
        self.conn.rollback()
        self.logger.exception('Database error, rollback initiated.')
        self.conn.close()
        self.logger.debug('db successfully closed.')
        sys.exit()

    def addExampleData(self):
        try:
            self.c.execute('''INSERT INTO nameTable VALUES
                           ('Test person 5',
                            'False',
                            'False',
                            ?,
                            ?,
                            0)''',
                           (datetime.date.today(),
                            datetime.date.today() -
                            datetime.timedelta(days=10)))

            self.c.execute('''INSERT INTO nameTable VALUES
                           ('Test person 6',
                           'False',
                           'False',
                            ?,
                            ?,
                            0)''',
                            (datetime.date.today(),
                            datetime.date.today()- datetime.timedelta(days=100)))

            self.c.execute('''INSERT INTO nameTable VALUES
                        ('Test person 7',
                        'False',
                        'False',
                        ?,
                        ?,
                        0)''',
                        (datetime.date.today(),
                         datetime.date.today()- datetime.timedelta(days=1000)))

            self.c.execute('''INSERT INTO nameTable VALUES
                        ('Test person 8',
                        'False',
                        'False',
                        ?,
                        ?,
                        0)''',
                        (datetime.date.today(),
                         datetime.date.today()- datetime.timedelta(days=10000)))

            self.conn.commit()
            self.logger.debug('Example records added, db saved')
        except Exception:
            return

    def getUnprayedList(self):
        #Returns a list of all records not yet prayed for
        try:
            self.c.execute('''SELECT name FROM nameTable WHERE prayedFor = 'False' ''')
            unprayedList = self.c.fetchall()
            if len(unprayedList) < 3:
                self.resetNames()
                self.c.execute('''SELECT name FROM nameTable WHERE prayedFor = 'False' ''')
                unprayedList = self.c.fetchall()
            return unprayedList
        except Exception:
            self.handleError()

    def pickRandomNames(self, unprayedList):
        #Set current Active names to False, pick 3 new names and set them Active
        try:
            self.c.execute('''UPDATE nameTable SET active = 'False'
                      WHERE active = 'True' ''')
            tupleList = random.sample(unprayedList,3)
            newNames = []
            for nameTuple in tupleList:
                newNames.append(nameTuple[0])
            for name in newNames:
                self.c.execute('''UPDATE nameTable SET active = 'True'
                              WHERE name = ?''', (name,))
            self.conn.commit()
            self.logger.debug('New names picked and made active, Db saved')
            return newNames
        except Exception:
            self.handleError()

    def resetNames(self):
        logger.debug('Names reset')
        try:
            self.c.execute('''UPDATE nameTable SET prayedFor = 'False' ''')
        except Exception:
            self.handleError()

    def markNameAsPrayed(self, name):
        #Mark passed name as done: Prayed for = True, prayerCount +1
        #last = today's date
        try:
            self.c.execute('''SELECT prayerCount FROM nameTable
                            WHERE name = ? ''',(name,))
            count = self.c.fetchone()[0]
            count += 1
            self.c.execute('''UPDATE nameTable SET prayedFor = 'True',
                          last = ?, prayerCount = ? WHERE name = ?''',
                          (datetime.date.today(), count, name))
            self.conn.commit()
            self.logger.debug(str(name) + ' updated as prayed, Db saved')

        except Exception:
            self.handleError()

    def addNameToDatabase(self,name):
        try:
            self.c.execute('''INSERT INTO nameTable(name, active, prayedFor,
                           created, last, prayerCount) VALUES(?, 'False',
                           'False', ?, ?, 0)''',
                           (name, datetime.date.today(), self.defaultDate))
            self.conn.commit()
        except sqlite3.IntegrityError:
            raise sqlite3.IntegrityError
        except Exception:
            self.handleError()

    def getActiveNames(self):
    #returns a list of tuples containing the 3 active names and if they are prayed for
        try:
            self.c.execute('''SELECT name, prayedFor FROM nameTable WHERE active
                            = 'True' ''')
            data = self.c.fetchall()
            names = []
            for nameTuple in data:
                names.append(nameTuple)
            if len(names) != 3:
                for i in range(3-len(names)):
                    names.append(('No name yet','False'))
            
            return names
        except Exception:
            self.handleError()

    def importToDatabase(self, file):
    # Only works with .csv
    # format is "name", "Bool Active", "Bool prayed for", "created", "last prayed", "no of hits"
    # or just names with return as the delimiter
    # Can't handle pre-existing names, whole import will fail if 1 name already exists
    
        with open(file, encoding = 'UTF-8') as namesFile:
            reader = csv.reader(namesFile)
            namesList = []
            for item in reader:
                namesList.append(item)
            print(namesList)

        try:
            if len(namesList[0]) == 1: # If import just a list of names - a list, not a list of lists
                logger.debug('Attempting to import ' + str(len(namesList)) + ' names')
            
                for name in namesList:
                    try:
                        self.c.execute('''INSERT INTO nameTable(name, active, prayedFor,
                                        created, last, prayerCount) VALUES (?, 'False',
                                        'False', ?, ?, 0)''',
                                        (name[0], datetime.date.today(),
                                        self.defaultDate))
                    except sqlite3.IntegrityError:
                        logger.info('name already exists, skipping import')
                        continue
                self.conn.commit()
                logger.debug('Import operation successful')
            else: # importing an exported .csv
                logger.debug('Attempting to import ' + str(len(namesList)) + ' records')
                for record in namesList:
                    try:
                        self.c.execute('''INSERT INTO nameTable (name, active, prayedFor,
                                        created, last, prayerCount) VALUES (?, 'False',
                                        ?, ?, ?, ?)''', (record[0], record[2], record[3],
                                                         record[4], record[5]))
                    except sqlite3.IntegrityError:
                        logger.info('name already exists, skipping import')
                        continue
                self.conn.commit()
                logger.debug('Import operation successful')

        except Exception:
            logger.debug('Unhandled error')
            self.handleError()

    def exportToFile(self, targetFilePath):
        self.c.execute('''SELECT name, active, prayedFor, created, last,
                        prayerCount FROM nameTable''')
        data = self.c.fetchall()
        with open(targetFilePath, 'w', encoding = 'UTF-8', newline = '') as exportFile:
            writer = csv.writer(exportFile, delimiter = ',',
                                quoting=csv.QUOTE_ALL)
            for row in data:
                writer.writerow(row)
                
    def closeDatabase(self):
        self.conn.close()
        self.logger.debug('db closed')
        closeLogging(self.logger)

if __name__ == "__main__":
    db = databaseConnect('prayer.db')
    db.addExampleData()
##    data = db.getUnprayedList()
##    newNames = db.pickRandomNames(data)
##    for usedName in newNames:
##        db.markNameAsPrayed(usedName)
    db.exportToFile(r'C:\Users\Steve Stanley\Documents\Computing\My_Scripts\Improved PrayerApp\Test data\exportExample.csv')
##    db.importToDatabase(r'C:\Users\Steve Stanley\Documents\Computing\My_Scripts\Improved PrayerApp\Test data\prayer names.csv')

##    print(newNames)
    db.closeDatabase()

