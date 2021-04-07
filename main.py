from db.backupper import Backupper
from db.db import Db
import pandas as pd
from db.table import Table
import sys
import getopt
import config

TEMP_DB = config.database
ORIGINAL_DB = config.original_database

def create_temp_db():
    backupper = Backupper(config.docker_container, config.user, config.password, config.host)
    backupper.backup(ORIGINAL_DB, 'fish_pot_2.sql')
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

def run():
    infile = 'data.csv'
    result_file = 'results.csv'
    alter_table_file = 'alter_tables.sql'

    df = pd.read_csv(infile)
    df = df.groupby('X')
    db = Db(TEMP_DB)
    total_errors = float(0)
    alter_statements = []

    with open('script.sql', 'w') as file:
        with open(result_file, 'w') as results:
            with open(alter_table_file, 'w') as alter_file:
                for d in df:
                    (table_name, data) = d
                    t = Table(db, table_name, data, sql_log=file, results_log=results, test_mode=False)
                    t.prepare_temp_table()
                    errors = t.test_temp_table()
                    if errors == 0:
                        statement = t.get_alter_table_for_original_table()
                        alter_statements.append(statement)
                        alter_file.write(statement)
                    total_errors += float(errors)
    if total_errors == 0:
        print(f'All tests passed: alter table file generated in {alter_table_file}')
        do_it = input(f"Do you want to apply the changes to the original database ({ORIGINAL_DB}) [yN]:")
        if do_it.lower() == 'y':
            print('Applying modifications to the original database')
            db = Db(ORIGINAL_DB)
            for statement in alter_statements:
                db.execute(statement)
            print('All done')
    else:
        print(f'Not all tests passed')
        sys.exit(1)



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

