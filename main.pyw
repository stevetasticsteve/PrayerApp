import sys
import os
import sqlite3
from PyQt5.QtWidgets import QMainWindow, QApplication, QMessageBox, QFileDialog, QPushButton
from PyQt5.QtWidgets import QInputDialog, QDialog, QGridLayout, QListWidget, QListWidgetItem
from PyQt5.QtGui import QIcon
from databaseFunc import DatabaseConnect
from prayerUI import *
from logSettings import createLogger, closeLogging

logger = createLogger(__name__)
logger.info('GUI started')


class MyApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.ui = Ui_MainWindow()
        self.setWindowIcon(QIcon('logo.png'))
        self.ui.setupUi(self)
        self.db = DatabaseConnect('prayer.db')
        self.ui.newNamesButton.clicked.connect(self.newNames)
        self.ui.prayedForAllButton.clicked.connect(self.markAllNames)
        self.startNames = self.db.get_active_names()
        self.ui.name1Label.setText(self.startNames[0][0])
        if self.startNames[0][1] == 'True':
            self.strikethrough(self.ui.name1Label)
        self.ui.name2Label.setText(self.startNames[1][0])
        if self.startNames[1][1] == 'True':
            self.strikethrough(self.ui.name2Label)
        self.ui.name3Label.setText(self.startNames[2][0])
        if self.startNames[2][1] == 'True':
            self.strikethrough(self.ui.name3Label)
        self.ui.name1Button.clicked.connect(lambda: self.markName(self.ui.name1Label.text(), self.ui.name1Label))
        self.ui.name2Button.clicked.connect(lambda: self.markName(self.ui.name2Label.text(), self.ui.name2Label))
        self.ui.name3Button.clicked.connect(lambda: self.markName(self.ui.name3Label.text(), self.ui.name3Label))
        self.ui.actionImport.triggered.connect(self.importData)
        self.ui.actionExport.triggered.connect(self.exportData)
        self.ui.actionQuit.triggered.connect(self.close)
        self.ui.actionAdd_new_name.triggered.connect(self.addName)
        self.ui.actionEdit_names.triggered.connect(self.editName)
        self.ui.actionReset_names.triggered.connect(self.resetNames)

    def errorHandling(self):
        logger.exception('Fatal Error:')
        QMessageBox.about(self, 'Error', 'A fatal error has occured, '
                          'check the log for details')
        sys.exit()

    def closeEvent(self, event):
        logger.debug('Close event')
        self.db.close_database()
        app.quit()

    def newNames(self):
        try:
            logger.debug('newNames called')
            newNames = self.db.pick_random_names(self.db.get_unprayed_list())
            logger.debug('new names = ' + str(newNames))
            self.ui.name1Label.setText(newNames[0])
            self.ui.name2Label.setText(newNames[1])
            self.ui.name3Label.setText(newNames[2])
            labels = [self.ui.name1Label, self.ui.name2Label, self.ui.name3Label]
            for i in labels:
                f = i.font()
                f.setStrikeOut(False)
                i.setFont(f)
        except Exception:
            self.errorHandling()

    def markAllNames(self):
        logger.debug('markAllNames called')
        try:
            labels = [self.ui.name1Label, self.ui.name2Label, self.ui.name3Label]
            for i in labels:
                self.markName(i.text(), i)
        except Exception:
            self.errorHandling()

    def markName(self, name, item):
        if item.font().strikeOut():
            logger.debug('doing nothing')
            return
        logger.debug('markName called for ' + name)
        try:
            self.strikethrough(item)
            self.db.mark_name_as_prayed(item.text())
        except Exception:
            self.errorHandling()
            
    def strikethrough(self, item):
        logger.debug('strikethrough called')
        try:
            f = item.font()
            f.setStrikeOut(True)
            item.setFont(f)
        except Exception:
            self.errorHandling()
            
    def importData(self):
        logger.debug('Import called from GUI')
        try:
            fname, _ = QFileDialog.getOpenFileName(self,
                                                'Import names',
                                                os.path.expanduser('~\\Documents'),
                                                'CSV file (*.csv)')
            if fname:
                self.db.import_to_database(fname)
        except Exception:
            self.errorHandling()

    def exportData(self):
        logger.debug('Export called from GUI')
        try:
            file_name, _ = QFileDialog.getSaveFileName(self, 'Export .csv',
                                                    os.path.expanduser('~\\Documents'),
                                                    'CSV file (*.csv)')
            if file_name:
                self.db.export_to_file(file_name)
        except Exception:
            self.errorHandling()

    def addName(self):
        logger.debug('addName called')
        try:
            name, ok = QInputDialog.getText(self, 'Add an entry', 'Enter name: ')
            if name and ok:
                self.db.add_name_to_database(name)
                QMessageBox.about(self, 'Database updated', (str(name) + ' was added to database'))
                logger.debug(str(name) + ' added to database')
        except sqlite3.IntegrityError:
            logger.debug('Not unique name error')
            QMessageBox.about(self, 'Database  not updated', (str(name) + ' already in database'))
        except Exception:
            self.errorHandling()

    def editName(self):
        try:
            table = editScreenWidget(w, self.db)
            logger.debug('editName called')
        except Exception:
            self.errorHandling()

    def resetNames(self):
        try:
            logger.debug('resetNames called')
            self.db.reset_names()
            QMessageBox.about(self, 'Reset', 'Names reset')
        except Exception:
            self.errorHandling()

class editScreenWidget(QDialog):
    def __init__(self, window, db):
        QDialog.__init__(self, window)
        self.window = window
        self.setWindowTitle('Edit names')
        self.db = db
        self.setGeometry(100, 150, 400, 650)
        self.setModal(True)
        self.grid = QGridLayout()
        self.list = QListWidget()
        names = db.get_all_names()
        names.sort()
        self.original_names = names.copy()
        for i, name in enumerate(names):
            item = QListWidgetItem(name)
            item.setFlags(item.flags() | QtCore.Qt.ItemIsEditable)
            self.list.insertItem(i, item)


        self.ok_btn = QPushButton('Ok')
        self.ok_btn.pressed.connect(lambda: self.update_db(self.window))
        self.cancel_btn = QPushButton('Cancel')
        self.cancel_btn.pressed.connect(self.close)
        self.grid.addWidget(self.list, 0, 0, 1, 2)
        self.grid.addWidget(self.ok_btn, 1, 0)
        self.grid.addWidget(self.cancel_btn, 1, 1)
        self.setLayout(self.grid)
        self.show()

    def update_db(self, window):
        try:
            changed = {} # edited names, original name as key
            for i in range(self.list.count()):
                if self.list.item(i).text() != self.original_names[i]:
                    changed[self.original_names[i]] = self.list.item(i).text()
            self.db.update_name(changed)
            self.close()
            window.newNames()
        except Exception:
            logger.exception('Fatal error:')




if __name__ == '__main__':
    app = QApplication(sys.argv)
    w = MyApp()
    w.show()
    sys.exit(app.exec_())
