import os
import sqlite3

DB_DIR = os.path.join(os.path.dirname(__file__), 'dbs')
os.makedirs(DB_DIR, exist_ok=True)

def country_db_path(country_code: str):
    return os.path.join(DB_DIR, f'db_{country_code.lower()}.sqlite')

def init_country_db_if_missing(country_code: str):
    path = country_db_path(country_code)
    if not os.path.exists(path):
        conn = sqlite3.connect(path)
        cur = conn.cursor()
        cur.execute('''
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                email TEXT,
                phone TEXT,
                job_title TEXT,
                location TEXT,
                country TEXT
            )
        ''')
        # sample shifts table for local testing
        cur.execute('''
            CREATE TABLE shifts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_title TEXT,
                location TEXT,
                start_time TEXT,
                end_time TEXT,
                details TEXT
            )
        ''')
        # seed one sample shift
        cur.execute("""INSERT INTO shifts (job_title, location, start_time, end_time, details) VALUES (?, ?, ?, ?, ?)""", (
            'Nurse', 'Mumbai', '2025-10-25T08:00:00', '2025-10-25T16:00:00', 'Day shift at City Hospital'
        ))
        conn.commit()
        conn.close()

def get_db_connection(country_code: str):
    path = country_db_path(country_code)
    if not os.path.exists(path):
        init_country_db_if_missing(country_code)
    conn = sqlite3.connect(path, check_same_thread=False)
    return conn
