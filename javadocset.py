# -*- coding: ascii -*-
from __future__ import print_function
import collections
import os
import os.path
import re
import shutil
import sqlite3
import sys
from bs4 import BeautifulSoup, Tag

__author__ = 'ethan'


def _execute(cursor, *args):
    assert isinstance(cursor, sqlite3.Cursor)
    cursor.execute(*args)
    return cursor


class BetterCursor(sqlite3.Cursor):
    def iter_dicts(self):
        c_columns = tuple(i[0] for i in self.description)
        for row in self:
            yield {k: v for (k, v) in zip(c_columns, row)}


class Database(object):
    def __init__(self, db_filename, overwrite=True):
        self._conn = sqlite3.connect(db_filename)
        self._conn.row_factory = sqlite3.Row
        self._c = self._conn.cursor(BetterCursor)
        self._c2 = self._conn.cursor(BetterCursor)
        assert isinstance(self._c, BetterCursor)
        self._freeze_cursor = False
        self._table_names = set()
        self._tables = {}
        self._tables_view_keys = self._tables.viewkeys()
        #if overwrite:
        #    self._tables_to_replace = ['theme']
        #self.execute('''CREATE TABLE IF NOT EXISTS theme (selector text UNIQUE);''')
        self.write_buffer_values = []
        self.write_buffer_command = None
        self.commit()

    def close(self):
        self._conn.close()

    def commit(self):
        self.flush()
        self._conn.commit()
        assert not self.frozen
        self.update('table_names')
        self.update('tables')

    def execute(self, *args):
        _cur = self._c2 if self.frozen else self._c
        _execute(_cur, *args)
        return _cur

    def get_table(self, table_name):
        t1 = self.tables[table_name]
        assert isinstance(t1, Table)
        return t1

    def iter_cursor(self):
        self._freeze_cursor = True
        for row in self._c:
            yield row
        self._freeze_cursor = False

    def iter_cursor_to_dict(self):
        assert isinstance(self._c, BetterCursor)
        self._freeze_cursor = True
        for r in self._c.iter_dicts():
            yield r
        self._freeze_cursor = False

    @property
    def frozen(self):
        """
        Is the cursor frozen?
        :rtype : bool
        """
        return self._freeze_cursor

    @frozen.setter
    def frozen(self, value):
        """
        Freeze cursor1 and use cursor2 instead
        :type value: bool
        """
        assert isinstance(value, bool)
        self._freeze_cursor = value

    def insert_dict_as_row(self, input_dict, table_name):
        self.get_table(table_name).insert_dict_as_row(input_dict)

    def select_all_from_table(self, table_name):
        return self.get_table(table_name).select_all()

    def table_names_(self):
        return set(str(i[0]) for i in self.execute("SELECT name FROM sqlite_master WHERE type='table';"))

    def tables_(self):
        return ((t, Table(self, t)) for t in self._table_names)

    def update(self, *args):
        self.frozen = True
        self._table_names = self.table_names_()
        self._tables.update(self.tables_())
        self.frozen = False

    def flush(self):
        """
        Flush the buffer.

        Buffer implementation usually improves efficiency by ~ 20%!
        """
        try:
            assert isinstance(self.write_buffer_command, str)
            assert len(self.write_buffer_values) > 0
            self._c.executemany(self.write_buffer_command, self.write_buffer_values)
            self.write_buffer_values = []
            self.write_buffer_command = None
        except AssertionError:
            pass

    @property
    def table_names(self):
        try:
            self.frozen = True
            self._table_names.clear()
            self._table_names |= self.table_names_()
        finally:
            self.frozen = False
        return self._table_names

    @property
    def tables(self):
        return self._tables


class Table(object):
    """
    convenient class to bind common methods and properties of an SQLite table
    """
    def __init__(self, theme_db, name):
        assert isinstance(theme_db, Database)
        assert isinstance(name, str)
        self._db = theme_db
        self.__name = name
        self._column_cache = set()

    def clear(self):
        """
        Delete all rows in this table.
        """
        self._db.execute("DELETE FROM %s;" % self.name)

    def select_all(self):
        """
        Select all rows in this table.
        """
        return self._db.execute("SELECT * FROM %s;" % self.name)

    def init_columns(self, *columns):
        changes = 0
        c_l = self.columns
        for a in set(columns) - c_l:
            self.add_column(a)
            self._db.commit()
            changes += 1
        return bool(changes % 2)

    def insert_dict_as_row(self, input_dict):
        assert isinstance(input_dict, dict)
        k_view = input_dict.viewkeys()
        v_view = input_dict.viewvalues()
        slot_str = '(?' + (',?' * (len(v_view) - 1)) + ')'

        new_cols = self.init_columns(*k_view)
        sql_str = str("INSERT OR REPLACE INTO %s %s VALUES %s;") % (self.name, repr(tuple(k_view)), slot_str)

        if not new_cols and sql_str == self._db.write_buffer_command:
            self._db.write_buffer_values.append(tuple(v_view))

        else:
            self._db.flush()
            self._db.execute(sql_str, tuple(v_view))

        self._db.write_buffer_command = sql_str

    def add_column(self, column, type_name=None):
        """
        Add a column to this table

        :param column: The name of the new column.
        :param type_name: Optional. Specify the SQLite type for the new column.
        """
        assert isinstance(column, str)
        assert type_name is None or isinstance(type_name, str)
        self._db.execute("ALTER TABLE %s ADD COLUMN %s;" %
                         (self.name, " ".join([column, type_name]) if type_name else column))

    @property
    def name(self):
        return self.__name

    @property
    def columns(self):
        if not self._db.frozen:
            self._column_cache = {str(i[1]) for i in self._db.execute("PRAGMA table_info('%s');" % self.name)}
        return self._column_cache


# The following is a modified version of shutil.copytree
def copytree(src, dst, symlinks=False, ignore=None):
    """Recursively copy a directory tree using copy2().

    The destination directory must not already exist.
    If exception(s) occur, an Error is raised with a list of reasons.

    If the optional symlinks flag is true, symbolic links in the
    source tree result in symbolic links in the destination tree; if
    it is false, the contents of the files pointed to by symbolic
    links are copied.

    The optional ignore argument is a callable. If given, it
    is called with the `src` parameter, which is the directory
    being visited by copytree(), and `names` which is the list of
    `src` contents, as returned by os.listdir():

        callable(src, names) -> ignored_names

    Since copytree() is called recursively, the callable will be
    called once for each directory that is copied. It returns a
    list of names relative to the `src` directory that should
    not be copied.

    XXX Consider this example code rather than the ultimate tool.

    """
    names = os.listdir(src)
    if ignore is not None:
        ignored_names = ignore(src, names)
    else:
        ignored_names = set()

    os.makedirs(dst)
    for name in names:
        if name in ignored_names:
            continue
        srcname = os.path.join(src, name)
        dstname = os.path.join(dst, name)
        try:
            if symlinks and os.path.islink(srcname):
                linkto = os.readlink(srcname)
                os.symlink(linkto, dstname)
            elif os.path.isdir(srcname):
                copytree(srcname, dstname, symlinks, ignore)
            else:
                # Will raise a SpecialFileError for unsupported file types
                shutil.copy2(srcname, dstname)
        # catch the Error from the recursive copytree so that we can
        # continue with other files
        except shutil.Error as err:
            print("\nCould not copy {}, error message: {}".format(srcname, str(err)))
        except EnvironmentError as why:
            print("\nCould not copy {}, error message: {}".format(srcname, str(why)))
    try:
        shutil.copystat(src, dst)
    except OSError as why:
        if WindowsError is not None and isinstance(why, WindowsError):
            # Copying file access times may fail on Windows
            pass
        else:
            print("\nCould not copystat {}, error message: {}".format(src, str(why)))


class DHIndexer(object):
    OVERVIEW_SUMMARY_FN = "overview-summary.html"

    def __init__(self):
        self.apiPath = None  # type: str
        self.workingDir = None  # type: str
        self.docsetName = None  # type: str
        self.docsetPath = None  # type: str

        self.contentsDir = None  # type: str
        self.resourcesDir = None  # type: str
        self.documentsDir = None  # type: str

        self.hasMultipleIndexes = None  # type: bool
        self.toIndex = collections.deque()

        self.soup = None  # type: BeautifulSoup
        self.soupFn = None  # type: str
        self.added = None  # type: list
        self.db = None  # type: FMDatabase.Database

        arguments = sys.argv
        if (len(arguments) == 2) and (arguments[1] == "--help"):
            self.printUsage()
            sys.exit(0)
        if len(arguments) != 3:
            print("Error: too {} arguments".format("many" if (len(arguments) > 3) else "few"))
            self.printUsage()
            sys.exit(1)
        print("Creating docset structure...")
        _name = arguments[1]  # type: str
        _path = arguments[2]  # type: str
        self.workingDir = os.getcwd()
        if not os.path.isabs(_path):
            _path = os.path.join(self.workingDir, _path)
        _path = os.path.abspath(_path)
        self.apiPath = _path
        self.docsetName = _name
        self.docsetPath = os.path.join(self.workingDir, "{}.docset".format(_name))
        self.contentsDir = os.path.join(self.docsetPath, "Contents")
        self.resourcesDir = os.path.join(self.contentsDir, "Resources")
        self.documentsDir = os.path.join(self.resourcesDir, "Documents")
        if os.path.exists(self.docsetPath):
            shutil.rmtree(self.docsetPath, ignore_errors=True)
        docsetIndexFile = None
        summaryPath = os.path.join(self.apiPath, self.OVERVIEW_SUMMARY_FN)
        foundSummary = False
        if not os.path.exists(summaryPath):
            for root, dirs, files in os.walk(self.apiPath):
                if self.OVERVIEW_SUMMARY_FN in files:
                    self.apiPath = os.path.abspath(root)
                    foundSummary = True
                    break
        else:
            foundSummary = True
        if foundSummary:
            docsetIndexFile = self.OVERVIEW_SUMMARY_FN
        if os.path.exists(os.path.join(self.apiPath, "index-files")):
            if docsetIndexFile is None:
                docsetIndexFile = "index-files/index-1.html"
            self.hasMultipleIndexes = True
        print("done")
        self.copyFiles()
        self.toIndex = collections.deque()
        if (not self.hasMultipleIndexes) and (os.path.exists(os.path.join(self.documentsDir, "index-all.html"))):
            self.toIndex.append(os.path.join(self.documentsDir, "index-all.html"))
            if docsetIndexFile is None:
                docsetIndexFile = "index-all.html"
        else:
            indexFilesPath = os.path.join(self.documentsDir, "index-files")
            for root, dirs, files in os.walk(indexFilesPath):
                for indexFile in files:  # type: str
                    if indexFile.startswith("index-") and indexFile.endswith(".html"):
                        self.toIndex.append(os.path.join(root, indexFile))
        if 0 == len(self.toIndex):
            print("\nError: The API folder you specified does not contain any index files "
                  "(either a index-all.html file or a index-files folder) and is not valid. "
                  "Please contact the developer if you receive this error by mistake.\n")
            self.printUsage()
            sys.exit(1)
        self.writeInfoPlist(docsetIndexFile)
        self.startIndexing()

    def writeInfoPlist(self, docsetIndexFile):
        """:type docsetIndexFile: str"""
        platform = self.docsetName.split(" ")[0].lower()
        plistStr = str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                       "<plist version=\"1.0\">"
                       "<dict>"
                       "<key>CFBundleIdentifier</key>"
                       "<string>{}</string>"
                       "<key>CFBundleName</key>"
                       "<string>{}</string>"
                       "<key>DocSetPlatformFamily</key>"
                       "<string>{}</string>"
                       "<key>dashIndexFilePath</key>"
                       "<string>{}</string>"
                       "<key>DashDocSetFamily</key>"
                       "<string>java</string>"
                       "<key>isDashDocset</key>"
                       "<true/>"
                       "</dict></plist>".format(platform, self.docsetName, platform, docsetIndexFile))
        with open(os.path.join(self.contentsDir, "Info.plist"), 'wb') as fd:
            fd.write(plistStr.encode('utf-8'))

    def copyFiles(self):
        print("Copying files...", end="")
        copytree(self.apiPath, self.documentsDir)
        print("done")

    @property
    def dbPath(self):
        return os.path.join(self.resourcesDir, "docSet.dsidx")

    def startIndexing(self):
        print("Start indexing...")
        self.added = []
        self.soup = None
        self.soupFn = None
        self.initDB()
        self.step()

    def step(self):
        if 0 == len(self.toIndex):
            self.db.commit()
            self.db.close()
            print("All done!")
            sys.exit(0)
        else:
            nextPath = self.toIndex.popleft()  # type: str
            print("Indexing {}...".format(os.path.basename(nextPath)))
            with open(nextPath, 'r') as fdIn:
                self.soup = BeautifulSoup(fdIn.read())
                self.soupFn = nextPath
            self.parseEntries()
            self.step()

    def parseEntries(self):
        for anchor in self.soup.find_all(u"a"):  # type: Tag
            parent = anchor.parent  # type: Tag
            if next(parent.children) != anchor:
                continue
            if re.match(r"^(span|code|i|b)$", str(parent.name), re.IGNORECASE):
                parent = parent.parent  # type: Tag
                if next(parent.children) != anchor.parent:
                    continue
            if not re.match(r"dt", str(parent.name), re.IGNORECASE):
                continue
            _text = str(parent.get_text())
            _textLower = _text.lower()
            _type = None
            _name = str(anchor.get_text())
            _dtClassName = str(parent.attrs.get("class", ""))
            if ("class in" in _textLower) or ("- class" in _textLower) or _dtClassName.endswith("class"):
                _type = "Class"
            elif ("static method in" in _textLower) or _dtClassName.endswith("method"):
                _type = "Method"
            elif ("static variable in" in _textLower) or _dtClassName.endswith("field") or ("field in" in _textLower):
                _type = "Field"
            elif ("constructor" in _textLower) or _dtClassName.endswith("constructor"):
                _type = "Constructor"
            elif "method in" in _textLower:
                _type = "Method"
            elif "variable in" in _textLower:
                _type = "Field"
            elif ("interface in" in _textLower) or ("- interface" in _textLower) or _dtClassName.endswith("interface"):
                _type = "Interface"
            elif ("exception in" in _textLower) or ("- exception" in _textLower) or _dtClassName.endswith("exception"):
                _type = "Exception"
            elif ("error in" in _textLower) or ("- error" in _textLower) or _dtClassName.endswith("error"):
                _type = "Error"
            elif ("enum in" in _textLower) or ("- enum" in _textLower) or _dtClassName.endswith("enum"):
                _type = "Enum"
            elif "trait in" in _textLower:
                _type = "Trait"
            elif "script in" in _textLower:
                _type = "Script"
            elif ("annotation type" in _textLower) or _dtClassName.endswith("annotation"):
                _type = "Notation"
            elif ("package" in _textLower) or _dtClassName.endswith("package"):
                _type = "Package"
            else:
                print("\nWarning: could not determine type for {}. Please tell the developer about this!".format(_name))
                print("\n{} and {}".format(_text, _dtClassName))
                continue
            _path = os.path.join(os.path.dirname(self.soupFn), anchor["href"])
            _path = os.path.relpath(_path, self.documentsDir)
            self.insertName(_name, _type, _path)

    def insertName(self, name_, type_, path_):
        if len(name_) > 200:
            # there's a bug in SQLite which causes it to sometimes hang on entries with > 200 chars
            name_ = name_[:200]
        parsedPath = path_
        if "#" in parsedPath:
            parsedPath = parsedPath.partition("#")[0]
        add = "{}{}{}".format(name_, type_, parsedPath)
        if add not in self.added:
            # print("adding {}".format(add))
            self.added.append(add)
            self.db.execute("INSERT INTO searchIndex(name, type, path) VALUES (?, ?, ?)", (name_, type_, path_))

    def initDB(self):
        self.db = Database(self.dbPath)
        self.db.execute("CREATE TABLE searchIndex(id INTEGER PRIMARY KEY, name TEXT, type TEXT, path TEXT)")
        self.db.update()

    @staticmethod
    def printUsage():
        print("Usage: javadocset <docset name> <javadoc API folder>\n"
              "<docset name> - anything you want\n"
              "<javadoc API folder> - the path of the javadoc API folder you want to index")


if __name__ == '__main__':
    DHIndexer()
