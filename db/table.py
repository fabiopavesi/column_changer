import sys
import time

from db.column import Column

changes = {
    'A': 'DECIMAL(26,5) NOT NULL',
    'B': 'DOUBLE NOT NULL',
    'C': 'DOUBLE NULL DEFAULT NULL'
}

all_at_once = False
new_column_suffix = '_newcolumn'

class Table:
    def __init__(self, db, table_name, df, results_log=None, sql_log=None, test_mode=False):
        self.log_file = results_log
        self.script_log = sql_log
        self.test_mode = test_mode
        self.table_name = table_name.lower()
        self.df = df
        self.db = db
        self.fields = []
        self.columns = []
        for index, row in self.df.iterrows():
            column = Column(self.db, self, row.Y.lower(), row.siglaModifica)
            self.columns.append(column)
            # print(self.db.db_name, row.Y.lower(), column.get_original_definition(), column.get_modified_definition())
            self.fields.append({
                'field': row.Y.lower(),
                'column': column,
                'siglaModifica': row.siglaModifica
            })

    def log_script(self, message):
        if self.script_log is not None:
            self.script_log.write(message)

    def log_results(self, message):
        if self.log_file is not None:
            self.log_file.write(message)

    def original_column_name(self, column_name):
        return f'{column_name}'

    def new_column_name(self, column_name):
        return f'{column_name}{new_column_suffix}'

    def prepare_temp_table(self):
        alters = []
        updates = []
        errors = []
        for column in self.columns:
            alters.append(column.get_add_table_column_sql(self.new_column_name(column.column_name)))
            updates.append(column.get_copy_table_column_value_sql(self.new_column_name(column.column_name)))
            errors.append(column.get_error_sql(self.new_column_name(column.column_name)))

        alter_sql = 'ALTER TABLE ' + self.table_name + ' ' + ',\n '.join(alters)
        update_sql = 'UPDATE ' + self.table_name + ' SET ' + ',\n '.join(updates)
        errors_sql = 'SELECT ' + '\n + '.join(errors) + ' AS a FROM ' + self.table_name
        self.log_script(alter_sql + ";\n")
        self.log_script(update_sql + ";\n")
        self.log_script(errors_sql + ";\n")
        if self.test_mode:
            print(alter_sql)
            print(update_sql)
            print(errors_sql)
        else:
            # print('actually running')
            # print(alter_sql)
            self.db.execute(alter_sql)
            # print(update_sql)
            self.db.execute(update_sql)
            self.db.commit()
            # print(errors_sql)
            results = self.db.query(errors_sql)
            total_error = 0
            for row in results:
                total_error = row[0]
                print('Total error for', self.table_name, row[0])
            self.log_results(self.table_name + ';' + str(total_error) + '\n')
