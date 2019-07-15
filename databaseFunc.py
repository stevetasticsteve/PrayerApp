import sqlite3
import sys
import datetime
import random
import csv
from logSettings import createLogger, closeLogging

logger = createLogger(__name__)


class DatabaseConnect:

    def __init__(self, db_name):
        try:
            self.conn = sqlite3.connect(db_name,
                                        detect_types=sqlite3.PARSE_DECLTYPES |
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
            self.defaultDate = datetime.date(2000, 1, 1)  # A not prayed for placeholder

        except Exception:
            self.logger.critical('__init__ error')

    def handle_error(self):
        self.conn.rollback()
        self.logger.exception('Database error, rollback initiated.')
        self.conn.close()
        self.logger.debug('db successfully closed.')
        sys.exit()

    def add_example_data(self):
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
                            datetime.date.today() - datetime.timedelta(days=100)))

            self.c.execute('''INSERT INTO nameTable VALUES
                        ('Test person 7',
                        'False',
                        'False',
                        ?,
                        ?,
                        0)''',
                           (datetime.date.today(),
                            datetime.date.today() - datetime.timedelta(days=1000)))

            self.c.execute('''INSERT INTO nameTable VALUES
                        ('Test person 8',
                        'False',
                        'False',
                        ?,
                        ?,
                        0)''',
                           (datetime.date.today(),
                            datetime.date.today() - datetime.timedelta(days=10000)))

            self.conn.commit()
            self.logger.debug('Example records added, db saved')
        except Exception:
            return

    def get_unprayed_list(self):
        # Returns a list of all records not yet prayed for
        try:
            self.c.execute('''SELECT name FROM nameTable WHERE prayedFor = 'False' ''')
            unprayed_list = self.c.fetchall()
            if len(unprayed_list) < 3:
                self.reset_names()
                self.c.execute('''SELECT name FROM nameTable WHERE prayedFor = 'False' ''')
                unprayed_list = self.c.fetchall()
            return unprayed_list
        except Exception:
            self.handle_error()

    def pick_random_names(self, unprayed_list):
        # Set current Active names to False, pick 3 new names and set them Active
        try:
            self.c.execute('''UPDATE nameTable SET active = 'False'
                      WHERE active = 'True' ''')
            tuple_list = random.sample(unprayed_list, 3)
            new_names = []
            for nameTuple in tuple_list:
                new_names.append(nameTuple[0])
            for name in new_names:
                self.c.execute('''UPDATE nameTable SET active = 'True'
                              WHERE name = ?''', (name,))
            self.conn.commit()
            self.logger.debug('New names picked and made active, Db saved')
            return new_names
        except Exception:
            self.handle_error()

    def reset_names(self):
        logger.debug('Names reset')
        try:
            self.c.execute('''UPDATE nameTable SET prayedFor = 'False' ''')
        except Exception:
            self.handle_error()

    def mark_name_as_prayed(self, name):
        # Mark passed name as done: Prayed for = True, prayerCount +1
        # last = today's date
        try:
            self.c.execute('''SELECT prayerCount FROM nameTable
                            WHERE name = ? ''', (name,))
            count = int(self.c.fetchone()[0])
            count += 1
            self.c.execute('''UPDATE nameTable SET prayedFor = 'True',
                          last = ?, prayerCount = ? WHERE name = ?''',
                           (datetime.date.today(), count, name))
            self.conn.commit()
            self.logger.debug(str(name) + ' updated as prayed, Db saved')

        except Exception:
            self.handle_error()

    def add_name_to_database(self, name):
        try:
            self.c.execute('''INSERT INTO nameTable(name, active, prayedFor,
                           created, last, prayerCount) VALUES(?, 'False',
                           'False', ?, ?, 0)''',
                           (name, datetime.date.today(), self.defaultDate))
            self.conn.commit()
        except sqlite3.IntegrityError:
            raise sqlite3.IntegrityError
        except Exception:
            self.handle_error()

    def get_active_names(self):
        # returns a list of tuples containing the 3 active names and if they are prayed for
        try:
            self.c.execute('''SELECT name, prayedFor FROM nameTable WHERE active
                            = 'True' ''')
            data = self.c.fetchall()
            names = []
            for nameTuple in data:
                names.append(nameTuple)
            if len(names) != 3:
                for i in range(3 - len(names)):
                    names.append(('No name yet', 'False'))

            return names
        except Exception:
            self.handle_error()

    def import_to_database(self, file):
        # Only works with .csv
        # format is "name", "Bool Active", "Bool prayed for", "created", "last prayed", "no of hits"
        # or just names with return as the delimiter
        # Can't handle pre-existing names, whole import will fail if 1 name already exists

        with open(file, encoding='UTF-8') as namesFile:
            reader = csv.reader(namesFile)
            names_list = []
            for item in reader:
                names_list.append(item)

        try:
            if len(names_list[0]) == 1:  # If import just a list of names - a list, not a list of lists
                logger.debug('Attempting to import ' + str(len(names_list)) + ' names')

                for name in names_list:
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
            else:  # importing an exported .csv
                logger.debug('Attempting to import ' + str(len(names_list)) + ' records')
                for record in names_list:
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
            self.handle_error()

    def export_to_file(self, target_file_path):
        self.c.execute('''SELECT name, active, prayedFor, created, last,
                        prayerCount FROM nameTable''')
        data = self.c.fetchall()
        with open(target_file_path, 'w', encoding='UTF-8', newline='') as exportFile:
            writer = csv.writer(exportFile, delimiter=',',
                                quoting=csv.QUOTE_ALL)
            for row in data:
                writer.writerow(row)

    def get_all_names(self):
        self.c.execute('''SELECT name From nameTable''')
        data = self.c.fetchall()
        result = []
        for item in data:
            result.append(item[0])
        return result

    def update_name(self,changed_names):
        # takes a dictionary with old names as keys to new values
        for name in changed_names:
            logger.info('Changing ' + name + ' to ' + changed_names[name])
            self.c.execute('''UPDATE nameTable SET name=? WHERE name=?''',
                           (changed_names[name], name))
        self.conn.commit()


    def close_database(self):
        self.conn.close()
        self.logger.debug('db closed')
        closeLogging(self.logger)
#TODO take out except Exceptions