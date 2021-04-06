import sys

from executor import execute
# from config import docker_container


class Backupper:
    """
    Backup and restore implementation addressing a dockerised MySQL (or MariaDB) instance
    """
    def __init__(self, container, user, password):
        self.container = container
        self.user = user
        self.password = password

    def backup(self, db_name, filename='backup.sql'):
        print('Backing up db', db_name, '... ', end='')
        sys.stdout.flush()
        execute(f'docker exec -i {self.container} mysqldump --single-transaction -u {self.user} --password={self.password} {db_name} > {filename}')
        print('Done')

    def restore(self, db_name, filename='backup.sql'):
        print('Restoring to db', db_name, '... ', end='')
        sys.stdout.flush()
        execute(f'docker exec -i {self.container} mysql -u {self.user} --password={self.password} -e "DROP DATABASE IF EXISTS {db_name}"')
        execute(f'docker exec -i {self.container} mysql -u {self.user} --password={self.password} -e "CREATE DATABASE {db_name}"')
        execute(f'docker exec -i {self.container} mysql -u {self.user} --password={self.password} {db_name} < {filename}')
        print('Done')
