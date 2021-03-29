from mysql.connector import connect
from config import *

class Db:
    def __init__(self):
        self.db = None

    def __del__(self):
        if self.db:
            self.db.close()

    def connect(self):
        self.db = connect(
            host=host,
            user=user,
            database=database,
            password=password
        )

    def drop_temp_tables(self):
        result = self.query("select table_name from information_schema.tables where table_schema = 'temp'")
        for row in result:
            print('drop table', 'temp.' + row[0])
            self.execute('drop table temp.' + row[0])

    def query(self, sql):
        self.connect()
        cursor = self.db.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        self.db.close()
        return results

    def execute(self, sql):
        self.connect()
        cursor = self.db.cursor()
        cursor.execute(sql)
        self.db.close()
