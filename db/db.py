from mysql.connector import connect
from config import *

class Db:
    def __init__(self, db_name):
        self.db = None
        self.db_name = db_name

    def __del__(self):
        if self.db:
            self.db.close()

    def connect(self):
        self.db = connect(
            host=host,
            user=user,
            database=self.db_name,
            password=password
        )

    def query(self, sql):
        self.connect()
        cursor = self.db.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        # self.db.close()
        return results

    def execute(self, sql):
        self.connect()
        cursor = self.db.cursor()
        cursor.execute(sql)
        # self.db.close()

    def commit(self):
        self.db.commit()
