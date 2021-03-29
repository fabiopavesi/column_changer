from db.db import Db
import pandas as pd
from db.table import Table
import argparse
import sys
import getopt

def test():
    infile = 'data.csv'
    df = pd.read_csv(infile)
    df = df.groupby('X')
    db = Db()
    with open('script.sql', 'w') as file:
        for d in df:
            (table_name, data) = d
            t = Table(table_name, data, file)
            sql = t.get_add_table_column_sql()
            file.write(sql + ';\n')
            sql = t.get_copy_table_column_value_sql()
            file.write(sql + ';\n')
            file.write('\n')
            t.by_record_error_sum(False)
            # print(table_name, sql)

def run():
    infile = 'data.csv'
    result_file = 'results.csv'

    df = pd.read_csv(infile)
    df = df.groupby('X')
    db = Db()
    db.drop_temp_tables()
    with open(result_file, 'w') as results:
        for d in df:
            (table_name, data) = d
            t = Table(table_name, data)
            try:
                sql = t.get_add_table_column_sql()
                db.execute(sql)
                sql = t.get_copy_table_column_value_sql()
                db.execute(sql)
                errors = t.by_record_error_sum()
                results.write(f'{table_name};{errors}\n')
                print('total error on table', table_name, errors)
            except Exception as e:
                print('Exception changing ', table_name)
                print(e)
                print('executed', sql)


test_mode = False
if __name__ == '__main__':

    opts, args = getopt.getopt(sys.argv[1:], 'tv')
    print(opts)
    for opt, arg in opts:
        if opt == '-t':
            print('test mode')
            test_mode = True

    if test_mode:
        test()
    else:
        run()

