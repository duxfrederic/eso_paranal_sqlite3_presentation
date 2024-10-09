import argparse
from pathlib import Path
import sqlite3
from astropy.io import fits
import pandas as pd


class DatabaseInterface:
    def __init__(self, db_path):
        # instance initialization: connect to the database at db_path.
        self.conn = sqlite3.connect(db_path)
        # create the tables: ok to run even if database and tables already exist.
        self.create_tables()

    def create_tables(self):
        cursor = self.conn.cursor()
        # table of all raw files.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS raw_files (
                id INTEGER PRIMARY KEY,
                path TEXT UNIQUE,
                binning TEXT,
                filter TEXT,
                category TEXT,
                type TEXT,
                mjd REAL,
                exposure_time REAL,
                read_speed TEXT,
                object_name TEXT,
                date TEXT
            )
        ''')
        # flats
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS flats (
                id INTEGER PRIMARY KEY,
                raw_file_id INTEGER UNIQUE,
                binning TEXT,
                filter TEXT,
                read_speed TEXT,
                FOREIGN KEY(raw_file_id) REFERENCES raw_files(id)
            )
        ''')
        # darks
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS darks (
                id INTEGER PRIMARY KEY,
                raw_file_id INTEGER UNIQUE,
                binning TEXT,
                read_speed TEXT,
                FOREIGN KEY(raw_file_id) REFERENCES raw_files(id)
            )
        ''')
        # science entries
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS science (
                id INTEGER PRIMARY KEY,
                raw_file_id INTEGER UNIQUE,
                unique_key TEXT UNIQUE,
                object_name TEXT,
                date TEXT,
                FOREIGN KEY(raw_file_id) REFERENCES raw_files(id)
            )
        ''')
        # reduced data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS reduced_data (
                id INTEGER PRIMARY KEY,
                science_unique_key TEXT UNIQUE,
                reduced_path TEXT UNIQUE,
                FOREIGN KEY(science_unique_key) REFERENCES science(unique_key)
            )
        ''')
        self.conn.commit()

    def raw_file_exists(self, path):
        # check in the database whether we've already inserted the file at the path at hand.
        cursor = self.conn.cursor()
        cursor.execute('SELECT id FROM raw_files WHERE path = ?', (str(path),))
        return cursor.fetchone() is not None  # evaluates to True if there is a matching file.

    def add_raw_file(self, file_info):
        # adding a raw file to the database
        if self.raw_file_exists(file_info['path']):
            return None
        # make a cursor, the object actually sending queries to the database
        cursor = self.conn.cursor()
        # insert the raw file.
        cursor.execute('''
            INSERT INTO raw_files
            (path, binning, filter, category, type, mjd, exposure_time, read_speed, object_name, date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            str(file_info['path']),
            file_info['binning'],
            file_info['filter'],
            file_info['category'],
            file_info['type'],
            file_info['mjd'],
            file_info['exposure_time'],
            file_info['read_speed'],
            file_info.get('object_name'),
            file_info.get('date')
        ))
        self.conn.commit()
        return cursor.lastrowid

    def add_flat(self, raw_file_id, binning, filter_name, read_speed):
        # when applicable, insert the flat information into the flats table.
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO flats (raw_file_id, binning, filter, read_speed)
            VALUES (?, ?, ?, ?)
        ''', (raw_file_id, binning, filter_name, read_speed))
        self.conn.commit()
        return cursor.lastrowid

    def add_dark(self, raw_file_id, binning, read_speed):
        # when applicable, insert the dark information into the darks table.
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO darks (raw_file_id, binning, read_speed)
            VALUES (?, ?, ?)
        ''', (raw_file_id, binning, read_speed))
        self.conn.commit()
        return cursor.lastrowid

    def add_science(self, raw_file_id, object_name, date):
        # when applicable, insert the science info into the science table.
        # we construct a unique id for this example: the name of the object and the date of the start of the OB.
        unique_key = f"{object_name}__{date}"
        cursor = self.conn.cursor()
        cursor.execute('SELECT id FROM science WHERE unique_key = ?', (unique_key,))
        if cursor.fetchone():
            print(f"Science entry with unique_key {unique_key} already exists.")
            return None
        cursor.execute('''
            INSERT INTO science (raw_file_id, unique_key, object_name, date)
            VALUES (?, ?, ?, ?)
        ''', (raw_file_id, unique_key, object_name, date))
        self.conn.commit()
        return cursor.lastrowid

    def add_reduced_data(self, science_unique_key, reduced_path):
        # to be called after successfully reducing a science
        cursor = self.conn.cursor()
        cursor.execute('INSERT OR IGNORE INTO reduced_data (science_unique_key, reduced_path) VALUES (?, ?)',
                       (science_unique_key, str(reduced_path)))
        self.conn.commit()
        return cursor.lastrowid

    def get_unreduced_science_files(self):
        # this query allows us to select all science files that do not have a corresponding reduced_data entry.
        query = '''
            SELECT unique_key
            FROM science
            LEFT JOIN reduced_data ON science.unique_key = reduced_data.science_unique_key
            WHERE reduced_data.id IS NULL
        '''
        df = pd.read_sql_query(query, self.conn)
        return df

    def get_files_for_science(self, science_id):
        """

        :param science_id:  unique_key, i.e. $objectname__$date
        :return: raw science file path, flats dataframe, darks dataframe
        """
        # get the science file
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT rf.path
            FROM science s
            JOIN raw_files rf on s.raw_file_id = rf.id
            WHERE s.unique_key = ?
        ''', (science_id,))
        science_raw_path, = cursor.fetchone()
        # get the parameters of the science file at hand.
        cursor.execute('''
            SELECT rf.binning, rf.read_speed, rf.filter
            FROM science s
            JOIN raw_files rf ON s.raw_file_id = rf.id
            WHERE s.unique_key = ?
        ''', (science_id,))
        result = cursor.fetchone()
        if not result:
            print(f"No science file with id {science_id}")
            return
        binning, read_speed, filter_name = result
        # Find matching flats
        flats_query = '''
            SELECT rf.*
            FROM flats f
            JOIN raw_files rf ON f.raw_file_id = rf.id
            WHERE f.binning = ?
            AND f.read_speed = ?
            AND f.filter = ?
        '''
        flats_df = pd.read_sql_query(flats_query, self.conn, params=(binning, read_speed, filter_name))
        # Find matching darks
        darks_query = '''
            SELECT rf.*
            FROM darks d
            JOIN raw_files rf ON d.raw_file_id = rf.id
            WHERE d.binning = ?
            AND d.read_speed = ?
        '''
        darks_df = pd.read_sql_query(darks_query, self.conn, params=(binning, read_speed))
        return science_raw_path, flats_df, darks_df


def register_fits_files(directory, db_path):
    path = Path(directory)
    db_interface = DatabaseInterface(db_path)

    fits_files = list(path.glob('*.fits*'))
    for fits_file in fits_files:
        try:
            with fits.open(fits_file) as hdulist:
                header = hdulist[0].header
        except Exception as e:
            print(f"Error reading {fits_file}: {e}")
            continue
        file_info = {}
        file_info['path'] = fits_file
        # Extract binning
        cdelt1 = int(header.get('CDELT1'))
        cdelt2 = int(header.get('CDELT2'))
        file_info['binning'] = f"{cdelt1}x{cdelt2}"
        file_info['filter'] = header.get('HIERARCH ESO INS FILT1 NAME')
        file_info['category'] = header.get('HIERARCH ESO DPR CATG')
        file_info['type'] = header.get('HIERARCH ESO DPR TYPE')
        file_info['mjd'] = header.get('MJD-OBS')
        file_info['exposure_time'] = header.get('EXPTIME')
        file_info['read_speed'] = header.get('HIERARCH ESO DET READ SPEED')
        file_info['date'] = header.get('DATE')
        file_info['object_name'] = header.get('OBJECT')
        # Add raw file to database
        raw_file_id = db_interface.add_raw_file(file_info)
        if raw_file_id is None:
            continue
        # Add to flats or darks table
        if file_info['category'] == 'CALIB':
            if 'FLAT' in file_info['type']:
                db_interface.add_flat(raw_file_id, file_info['binning'], file_info['filter'], file_info['read_speed'])
            elif file_info['type'] == 'DARK':
                db_interface.add_dark(raw_file_id, file_info['binning'], file_info['read_speed'])
            else:
                print(f"Unknown calibration type '{file_info['type']}' for file {fits_file}")
        elif file_info['category'] == 'SCIENCE':
            object_name = file_info['object_name']
            date = file_info['date']
            if object_name and date:
                db_interface.add_science(raw_file_id, object_name, date)
            else:
                print(f"Missing object_name or date for science file {fits_file}")
        else:
            print(f"Unknown category '{file_info['category']}' for file {fits_file}")


