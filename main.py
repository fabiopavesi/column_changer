from db.backupper import Backupper
from db.db import Db
import pandas as pd
from db.table import Table
import argparse
import sys
import getopt
import config

TEMP_DB = 'temp'

def create_temp_db():
    backupper = Backupper(config.docker_container, config.user, config.password, config.host)
    backupper.backup('fish_pot_2', 'fish_pot_2.sql')
    backupper.restore(TEMP_DB, 'fish_pot_2.sql')

def test():
    infile = 'data.csv'
    df = pd.read_csv(infile)
    df = df.groupby('X')
    db = Db(TEMP_DB)
    with open('script.sql', 'w') as file:
        for d in df:
            (table_name, data) = d
            t = Table(db, table_name, data, file, test_mode=True)
            t.prepare_temp_table()
            # sql = t.get_add_table_column_sql()
            # file.write(sql + ';\n')
            # sql = t.get_copy_table_column_value_sql()
            # file.write(sql + ';\n')
            # file.write('\n')
            # t.by_record_error_sum(False)
            # # print(table_name, sql)

def run():
    infile = 'data.csv'
    result_file = 'results.csv'

    df = pd.read_csv(infile)
    df = df.groupby('X')
    db = Db(TEMP_DB)
    with open('script.sql', 'w') as file:
        # with open(result_file, 'w') as results:
        for d in df:
            (table_name, data) = d
            t = Table(db, table_name, data, file, test_mode=False)
            t.prepare_temp_table()

            # (table_name, data) = d
            # t = Table(db, table_name, data)
            # sql = None
            # try:
            #     sql = t.get_add_table_column_sql()
            #     db.execute(sql)
            #     sql = t.get_copy_table_column_value_sql()
            #     db.execute(sql)
            #     errors = t.by_record_error_sum()
            #     results.write(f'{table_name};{errors}\n')
            #     print('total error on table', table_name, errors)
            # except Exception as e:
            #     print('Exception changing ', table_name)
            #     print(e)
            #     print('executed', sql)


test_mode = False
no_backups = False
if __name__ == '__main__':

    opts, args = getopt.getopt(sys.argv[1:], 'tvb')
    # print(opts)
    for opt, arg in opts:
        if opt == '-t':
            print('test mode')
            test_mode = True
        elif opt == '-b':
            print('no recreation of temp db')
            no_backups = True

    if not no_backups:
        create_temp_db()
    if test_mode:
        test()
    else:
        run()

