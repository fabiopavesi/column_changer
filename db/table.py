import sys

from db import Db
from db.column import Column

changes = {
    'A': 'DECIMAL(26,5) NOT NULL',
    'B': 'DOUBLE NOT NULL',
    'C': 'DOUBLE NULL DEFAULT NULL'
}

all_at_once = False
new_column_suffix = '_newcolumn'

class Table:
    def __init__(self, table_name, df, log_file = None):
        self.log_file = log_file
        self.table_name = table_name.lower()
        self.df = df
        self.db = Db()
        # self.create_temp_table()
        self.fields = []
        for index, row in self.df.iterrows():
            column = Column(self.db, self, row.Y.lower(), row.siglaModifica)
            print(row.Y.lower(), column.get_original_definition(), column.get_modified_definition())
            self.fields.append({
                'field': row.Y.lower(),
                'siglaModifica': row.siglaModifica
            })
        # print('table', self.table_name)
        # print('fields', self.fields)
    # print(table_name, df)

    def by_record_error_sum(self, execute=True):
        cross_field_error = 0
        print('checking table', self.table_name)
        field_sum_list = []

        for field in self.fields:
            field_sum_list.append('SUM(ABS(`{1}` - `{0}`))'.format(
                self.original_column_name(field["field"]),
                self.new_column_name(field['field'])
            ))

        sql = """
            select {0}
              from {1}""".format(
                ' + '.join(field_sum_list),
                self.table_name
            )

        if self.log_file is not None:
            self.log_file.write(sql)
        if (execute):
            result = self.db.query(sql)
            for row in result:
                for i in range(0, len(field_sum_list)):
                    cross_field_error += row[0]
                    # print('sum', field['field'], row[0])
                    print('.', end='')
                    sys.stdout.flush()
                    cross_field_error += row[0]

        return cross_field_error

    def total_error_sum(self):
        """
        For each modified column it sums original and modified column for all rows, then calculates the absolute value
        of the difference between the two sums

        :return:
        Sum of said absolutes differences
        """

        cross_field_error = 0
        print('checking table', self.table_name)
        field_sum_list = []
        for field in self.fields:
            # print('field', field)
            if all_at_once:
                field_sum_list.append(f'ABS(SUM(b.`{field["field"]}`) - SUM(a.`{field["field"]}`))')
            else:
                sql = """
                    select ABS(SUM(b.`{0}`) - SUM(a.`{0}`))
                      from fish_pot_2.{1} a, {1} b """.format(field["field"], self.table_name)
                # print('sql', sql)
                result = self.db.query(sql)
                for row in result:
                    for i in range(0, len(field_sum_list)):
                        cross_field_error += row[0]
                        # print('sum', field['field'], row[0])
                        print('.', end='')
                        sys.stdout.flush()
                        cross_field_error += row[0]

        if all_at_once:
            sql = """
                    select {0}
                      from fish_pot_2.{1} a, {1} b """.format(', '.join(field_sum_list), self.table_name)
            # print('sql', sql)
            result = self.db.query(sql)
            for row in result:
                for i in range(0, len(field_sum_list)):
                    cross_field_error += row[0]
                    # print('sum', field['field'], row[0])
                    print('.', end='')
                    sys.stdout.flush()
        print()
        print('total error', cross_field_error)
        return cross_field_error

    def has_column(self, column):
        columns = self.db.query(f"select * from information_schema.columns where column_name = '{column.lower()}' and table_name = '{self.table_name.lower()}' ")
        if len(columns) > 0:
            return True
        else:
            return False

    def original_column_name(self, column_name):
        return f'original_{column_name}'

    def new_column_name(self, column_name):
        return f'{column_name}{new_column_suffix}'

    def create_temp_table(self):
        # self.db.execute(f'create table if not exists {self.table_name} as select * from fish_pot_2.{self.table_name}')
        result = self.db.query(f'SHOW CREATE TABLE fish_pot_2.{self.table_name}')
        # print('result: ', result)
        for row in result:
            print('create table: ', row[1])
            self.db.execute(row[1])


    def get_alter_table_column_sql(self):
        # print(self.df)
        sql = f'alter table {self.table_name}\n'
        columns = []
        for row in self.fields:
            columns.append(f' MODIFY `{self.original_column_name(row["field"])}` {changes[row["siglaModifica"]]}')
        sql += ',\n'.join(columns)
        # print(sql)
        return sql

    def get_add_table_column_sql(self):
        # print(self.df)
        sql = f'alter table {self.table_name}\n'
        columns = []
        for row in self.fields:
            columns.append(f' ADD `{self.new_column_name(row["field"])}` {changes[row["siglaModifica"]]}')
            columns.append(f' RENAME COLUMN `{row["field"]}` TO `{self.original_column_name(row["field"])}`')
        sql += ',\n'.join(columns)
        # print(sql)
        return sql

    def get_copy_table_column_value_sql(self):
        # print(self.df)
        sql = f'UPDATE {self.table_name}\n SET '
        columns = []
        for row in self.fields:
            columns.append(f' `{self.new_column_name(row["field"])}` = `{self.original_column_name(row["field"])}`')
        sql += ',\n'.join(columns)
        # print(sql)
        return sql
