import sys

from executor import execute
# from config import docker_container


class Backupper:
    """
    Backup and restore implementation addressing a dockerised MySQL (or MariaDB) instance
    """
    def __init__(self, container, user, password, remote_docker_host=None):
        self.container = container
        self.user = user
        self.password = password
        self.remote_docker_host = remote_docker_host

    def backup(self, db_name, filename='backup.sql'):
        print('Backing up db', db_name, '... ', end='')
        sys.stdout.flush()
        execute(f'{"ssh " + self.user + "@" + self.remote_docker_host if self.remote_docker_host is not None else ""} docker exec -i {self.container} mysqldump --single-transaction -u {self.user} --password={self.password} {db_name} > {filename}')
        print('Done')

    def restore(self, db_name, filename='backup.sql'):
        print('Restoring to db', db_name, '... ', end='')
        sys.stdout.flush()
        execute(f'scp {filename} {self.user}@{self.remote_docker_host}:{filename}')
        drop_db_command = f'{"ssh " + self.user + "@" + self.remote_docker_host if self.remote_docker_host is not None else ""} \'docker exec -i {self.container} mysql -u {self.user} --password={self.password} -e "DROP DATABASE IF EXISTS {db_name}"\''
        create_db_command = f'{"ssh " + self.user + "@" + self.remote_docker_host if self.remote_docker_host is not None else ""} \'docker exec -i {self.container} mysql -u {self.user} --password={self.password} -e "CREATE DATABASE {db_name}"\''
        restore_command = f'{"ssh " + self.user + "@" + self.remote_docker_host if self.remote_docker_host is not None else ""} \'docker exec -i {self.container} mysql -u {self.user} --password={self.password} {db_name} < {filename}\''

        print('drop db', drop_db_command)
        print('create db', create_db_command)
        print('restore db', restore_command)

        print('Dropping previous db', end='... ')
        execute(drop_db_command)
        print('Creating empty db', end='... ')
        execute(create_db_command)
        print('Restoring db', end='... ')
        execute(restore_command)
        print('Done')
