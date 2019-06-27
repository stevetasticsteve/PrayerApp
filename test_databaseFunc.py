import unittest
import sqlite3
import databaseFunc
import os
import datetime
import logging
import logSettings

logging.disable(logging.CRITICAL)

class TestDatabase(unittest.TestCase):

    def setUp(self):
        self.db = databaseFunc.databaseConnect('test.db')

        self.db.c.execute('''CREATE TABLE IF NOT EXISTS nameTable(
                        name TEXT PRIMARY KEY,
                        active TEXT,
                        prayedFor TEXT,
                        created DATE,
                        last DATE,
                        prayerCount INTEGER)''')

        self.db.c.execute('''INSERT INTO nameTable VALUES
                    ('Test person 1',
                    'False',
                    'True',
                    ?,
                    ?,
                    0)''',
                    (datetime.date.today(),
                     datetime.date.today()- datetime.timedelta(days=10)))

        self.db.c.execute('''INSERT INTO nameTable VALUES
                    ('Test person 2',
                    'False',
                    'False',
                    ?,
                    ?,
                    0)''',
                    (datetime.date.today(),
                     datetime.date.today()- datetime.timedelta(days=100)))

        self.db.c.execute('''INSERT INTO nameTable VALUES
                    ('Test person 3',
                    'False',
                    'False',
                    ?,
                    ?,
                    0)''',
                    (datetime.date.today(),
                     datetime.date.today()- datetime.timedelta(days=1000)))

        self.db.c.execute('''INSERT INTO nameTable VALUES
                    ('Test person 4',
                    'False',
                    'False',
                    ?,
                    ?,
                    0)''',
                    (datetime.date.today(),
                     datetime.date.today()- datetime.timedelta(days=10000)))

    def tearDown(self):
        self.db.conn.close()
        os.remove(os.path.join(os.getcwd(), 'test.db'))
        logSettings.closeLogging(self.db.logger)

    def test_handleError(self):
        #Not sure this does anything... never calls function
        with self.assertRaises(sqlite3.OperationalError):
            self.db.c.execute('''INSERT INTO fakeTable VALUES ('pants') ''')

    def test_addExampleData(self):
        self.db.c.execute('''SELECT name, created From nameTable''')
        self.assertEqual(len(self.db.c.fetchall()),4)
        self.db.addExampleData()
        self.db.c.execute('''SELECT name, created From nameTable''')
        data = self.db.c.fetchall()
        names, dates = [], []
        for entry in data:
            names.append(entry[0])
            dates.append(entry[1])
        self.assertIn('Test person 5', names)
        self.assertIn('Test person 7', names)
        self.assertTrue(type(dates[0]) == datetime.date)
        self.assertEqual(datetime.date.today(), dates[1])
        self.assertEqual(len(data), 8)

    def test_getUnprayedList(self):
        unprayed = self.db.getUnprayedList()
        for name in unprayed:
            self.db.c.execute('''SELECT prayedFor FROM nameTable
                              WHERE name = ? ''',(name[0],))
            self.assertEqual(self.db.c.fetchone()[0], 'False')
        self.db.c.execute('''SELECT prayedFor FROM nameTable WHERE
                          name = 'Test person 1' ''')
        self.assertEqual(self.db.c.fetchone()[0], 'True')
        self.assertEqual(len(unprayed), 3)

        self.db.c.execute('''UPDATE nameTable SET prayedFor = 'True' ''')
        self.db.conn.commit()
        self.db.c.execute('''SELECT name FROM nameTable WHERE
                           prayedFor = 'True' ''')
        self.assertEqual(len(self.db.c.fetchall()),4)
        unprayed = self.db.getUnprayedList()
        self.db.c.execute('''SELECT name FROM nameTable WHERE
                           prayedFor = 'True' ''')
        self.assertEqual(len(self.db.c.fetchall()),0)

        for name in unprayed:
            self.db.c.execute('''SELECT prayedFor FROM nameTable
                              WHERE name = ? ''',(name[0],))
            self.assertEqual(self.db.c.fetchone()[0], 'False')
        self.db.c.execute('''SELECT prayedFor FROM nameTable WHERE
                          name = 'Test person 1' ''')
        self.assertEqual(self.db.c.fetchone()[0], 'False')
        self.assertEqual(len(unprayed), 4)
        

    def test_pickRandomNames(self):
        unprayedList = [('Test person 2',),('Test person 3',),
                        ('Test person 4',)]
        names = self.db.pickRandomNames(unprayedList)
        self.assertEqual(len(names), 3)
        self.assertNotIn('Test person 1', names)
        self.assertNotIn('Test person 5', names)
        self.assertIn('Test person 2', names)
        self.assertIn('Test person 4', names)
        self.db.c.execute('''SELECT name, active, prayedFor FROM
                            nameTable ''')
        data = self.db.c.fetchall()
        self.assertEqual(data[0][1],'False')
        self.assertEqual(data[0][2],'True')
        self.assertEqual(data[1][1],'True')
        self.assertEqual(data[1][2],'False')
        self.assertEqual(data[3][2],'False')
        self.db.c.execute('''SELECT name FROM nameTable WHERE
                          active = 'True' ''')
        self.assertEqual(len(self.db.c.fetchall()), 3)
        
    def test_resetNames(self):
        self.db.c.execute('''SELECT name from nameTable Where
                          prayedFor = 'False' ''')
        self.assertEqual(len(self.db.c.fetchall()),3)
        self.db.resetNames()
        self.db.c.execute('''SELECT name from nameTable Where
                          prayedFor = 'False' ''')
        self.assertEqual(len(self.db.c.fetchall()),4)

    def test_markNameAsPrayed(self):
        self.db.c.execute('''SELECT prayedFor, last, prayerCount FROM
                          nameTable WHERE name = 'Test person 2' ''')
        data = self.db.c.fetchone()
        self.assertEqual(data[0], 'False')
        self.assertNotEqual(data[1], datetime.date.today())
        self.assertEqual(data[2], 0)
        self.db.markNameAsPrayed('Test person 2')
        self.db.c.execute('''SELECT prayedFor, last, prayerCount FROM
                          nameTable WHERE name = 'Test person 2' ''')
        data = self.db.c.fetchone()
        self.assertEqual(data[0], 'True')
        self.assertEqual(data[1], datetime.date.today())
        self.assertEqual(data[2], 1)

    def test_getActiveNames(self):
        data = self.db.getActiveNames()
        self.assertEqual(len(data),3)
        self.assertEqual(data[0][0], 'No name yet')
        self.assertEqual(data[0][1], 'False')
        self.assertEqual(data[2][0], 'No name yet')
        self.assertEqual(data[2][1], 'False')
        self.assertEqual(type(data[0]), tuple)
        self.db.c.execute('''UPDATE nameTable SET active = 'True'
                         WHERE name = 'Test person 1' ''')
        self.db.conn.commit()
        data = self.db.getActiveNames()
        self.assertEqual(len(data),3)
        self.assertEqual(data[0][0], 'Test person 1')
        self.assertEqual(data[0][1], 'True')
        self.assertEqual(data[2][0], 'No name yet')
        self.assertEqual(data[2][1], 'False')
        self.db.c.execute('''UPDATE nameTable SET active = 'True'
                         WHERE name = 'Test person 2' ''')
        self.db.conn.commit()
        data = self.db.getActiveNames()
        self.assertEqual(len(data),3)
        self.assertEqual(data[1][0], 'Test person 2')
        self.assertEqual(data[1][1], 'False')

    def test_addNameToDatabase(self):
        self.db.c.execute('''SELECT name FROM nameTable''')
        data = self.db.c.fetchall()
        for name in data:
            self.assertNotIn('Spiderman',name[0][0])
        self.db.addNameToDatabase('Spiderman')
        self.db.c.execute('''SELECT name, active, prayedFor, prayerCount,
                          created, last FROM nameTable WHERE name =
                          'Spiderman' ''')
        data = self.db.c.fetchone()
        self.assertEqual(len(data),6)
        self.assertEqual(data[0],'Spiderman')
        self.assertEqual(data[1],'False')
        self.assertEqual(data[2],'False')
        self.assertEqual(data[3],0)
        self.assertEqual(data[4], datetime.date.today())
        self.assertEqual(data[5], self.db.defaultDate)

    def test_importToDatabase(self):
        testFile = os.path.join(os.getcwd(),'Test data','PlainNames.csv')
        self.db.importToDatabase(testFile)
        self.db.c.execute('''SELECT name FROM nameTable''')
        data = self.db.c.fetchall()
        self.assertEqual(len(data), 8)
        self.assertEqual(data[0][0], 'Test person 1')
        self.assertEqual(data[7][0], 'Test person 8')

        additonal_names = os.path.join(os.getcwd(),'Test data','additionalPlainNames.csv')
        self.db.importToDatabase(additonal_names)
        self.db.c.execute('''SELECT name FROM nameTable''')
        data = self.db.c.fetchall()
        self.assertEqual(len(data), 9)
        self.assertEqual(data[8][0], 'Test person 9')

    def test_importExportFile(self):
        testFile = os.path.join(os.getcwd(),'Test data','exportExample.csv')
        self.db.importToDatabase(testFile)
        self.db.c.execute('''SELECT name, active, prayedFor,
                            created, prayerCount last FROM nameTable''')
        data = self.db.c.fetchall()
        self.assertEqual(len(data), 8)
        self.assertEqual(data[-1][0], 'Test person 8')
        self.assertEqual(data[7][3], datetime.date(2018, 11, 1))

        addition = os.path.join(os.getcwd(),'Test data','additionalExportData.csv')
        self.db.importToDatabase(addition)
        self.db.c.execute('''SELECT name, active, prayedFor,
                                    created, prayerCount last FROM nameTable''')
        data = self.db.c.fetchall()
        self.assertEqual(len(data), 9)
        self.assertEqual(data[-1][0], 'Test person 9')
if __name__ == '__main__':
    try:
        unittest.main()
    finally:
        input()
