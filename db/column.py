from db.error_evaluation_strategy import SumOfAbsoluteDiffences

changes = {
    'A': 'DECIMAL(26,5) NOT NULL',
    'B': 'DOUBLE NOT NULL',
    'C': 'DOUBLE NULL DEFAULT NULL'
}

class Column:

    def __init__(self, db, table, column_name, requested_change, error_evaluation_strategy=SumOfAbsoluteDiffences()):
        self.table = table
        self.db = db
        self.column_name = column_name
        self.definition = None
        self.requested_change = requested_change
        self.error_evaluation_strategy = error_evaluation_strategy
        self.parse_definition()

    def __repr__(self):
        return str({
            'table': self.table.table_name,
            'db': self.db.db_name,
            'column_name': self.column_name,
            'definition': self.definition,
            'requested_change': self.requested_change,
            'original_definition': self.get_original_definition(),
            'modified_definition': self.get_modified_definition(),
        })

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

    def get_alter_table_column_sql(self):
        columns = []
        return f' MODIFY `{self.column_name}` {self.get_modified_definition()}'

    def get_add_table_column_sql(self, new_name):
        # print(self.df)
        columns = []
        columns.append(f' ADD `{new_name}` {self.get_modified_definition()}')
        # columns.append(f' RENAME COLUMN `{row["field"]}` TO `{self.original_column_name(row["field"])}`')
        sql = ',\n'.join(columns)
        # print(sql)
        return sql

    def get_copy_table_column_value_sql(self, new_name):
        # print(self.df)
        return f' `{new_name}` = `{self.column_name}`'

    def get_error_sql(self, new_name):
        # return f' SUM(ABS(`{new_name}` - `{self.column_name}`))'
        return self.error_evaluation_strategy(self.column_name, new_name)
