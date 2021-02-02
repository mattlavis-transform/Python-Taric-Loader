import sys
import os
import logging
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import common.globals as g


class Database:
    """PostgreSQL Database class."""

    def __init__(self):
        self.conn = None

    def open_connection(self):
        """Connect to a Postgres database."""
        try:
            if(self.conn is None):
                self.conn = psycopg2.connect(
                    "dbname=" + g.app.DBASE + " user=postgres password=" + g.app.password)
        except psycopg2.DatabaseError as e:
            logging.error(e)
            sys.exit()
        finally:
            logging.info('Connection opened successfully.')

    def close_connection(self):
        self.conn = None

    def run_query(self, query, params=None):
        """Run a SQL query."""
        try:
            self.open_connection()
            with self.conn.cursor() as cur:
                if 'SELECT' in query.upper():
                    records = []
                    if params is None:
                        cur.execute(query)
                    else:
                        cur.execute(query, params)
                    result = cur.fetchall()
                    for row in result:
                        records.append(row)
                    cur.close()
                    return records
                else:
                    result = cur.execute(query)
                    self.conn.commit()
                    affected = f"{cur.rowcount} rows affected."
                    cur.close()
                    return affected
        except psycopg2.DatabaseError as e:
            print(e)
        finally:
            if self.conn:
                self.conn.close()
                logging.info('Database connection closed.')
