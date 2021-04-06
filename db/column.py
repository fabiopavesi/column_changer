changes = {
    'A': 'DECIMAL(26,5) NOT NULL',
    'B': 'DOUBLE NOT NULL',
    'C': 'DOUBLE NULL DEFAULT NULL'
}

class Column:

    def __init__(self, db, table, column_name, requested_change):
        self.table = table
        self.db = db
        self.column_name = column_name
        self.definition = None
        self.requested_change = requested_change
        self.parse_definition()

    def parse(self, row):
        self.definition = {
            'table_name': row[1],
            'column_name': row[2],
            'column_default': row[5],
            'is_nullable': row[6],
            'column_type': row[15]
        }

    def parse_definition(self):
        sql = f"SELECT * FROM information_schema.columns WHERE table_name = '{self.table.table_name}' AND column_name = '{self.column_name}'"
        response = self.db.query(sql)
        for row in response:
            self.parse(row)
            # print(row)
            # print('column definition', sql, self.definition)

    def get_original_definition(self):
        return f'{self.definition["column_type"]} {"NULL" if self.definition["is_nullable"] == "YES" else "NOT NULL"} {"DEFAULT " + str(self.definition["column_default"]) if self.definition["column_default"] else ""}'

    def get_modified_definition(self):
        return changes[self.requested_change]
