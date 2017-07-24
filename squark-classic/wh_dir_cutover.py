import os
import sys
import time
import logging
import logging.config

from os.path import join

from pywebhdfs.webhdfs import PyWebHdfsClient
import requests


LOGLEVEL = os.getenv('LOGLEVEL', 'INFO')
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': "%(asctime)s %(levelname)s %(module)s::%(funcName)s: %(message)s",
            'datefmt': '%H:%M:%S'
        }
    },
    'handlers': {
        'default': {'level': LOGLEVEL,
                    'class': 'ansiterm.ColorizingStreamHandler',
                    'formatter': 'standard'},
    },
    'loggers': {
        '': {
            'handlers': ['default'], 'level': LOGLEVEL, 'propagate': True
        },
    },
}

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger('wh-cutover')


# This the temporary location where new data is written before
# Being moved into the SQUARK_WAREHOUSE.
SQUARK_TEMP = os.environ['SQUARK_TEMP']

# This is the dir where data is accessed by Splinkr.
SQUARK_WAREHOUSE = os.environ['SQUARK_WAREHOUSE']

# And this is where current data gets moved once it's ready
# to replace.
SQUARK_ARCHIVE = os.environ['SQUARK_ARCHIVE']


class JenkinsClient:

    def __init__(self):
        hdfs_host = os.environ.get('HDFS_HOST','')
        hdfs_port = os.environ.get('HDFS_PORT','50070')
        hdfs_user = os.environ.get('HDFS_USER','jenkins')
        self.hdfs = PyWebHdfsClient(host=hdfs_host, port=hdfs_port, user_name=hdfs_user)

    def archive_dir(self, dirname):
        '''Move a dir from SQUARK_WAREHOUSE --> SQUARK_ARCHIVE
        '''
        archive_path = join(SQUARK_ARCHIVE, dirname)
        warehouse_path = join(SQUARK_WAREHOUSE, dirname)
        logger.info("Retiring file from warehoue to archive: %r", dirname)
        self.hdfs.delete_file_dir(archive_path, recursive=True)
        self.move_dir(warehouse_path, archive_path)

    def deploy_dir(self, dirname):
        '''Move a dir from SQUARK_TEMP --> SQUARK_WAREHOUSE
        '''
        warehouse_path = join(SQUARK_WAREHOUSE, dirname)
        temp_path = join(SQUARK_TEMP, dirname)
        logger.info("Promoting file from temp to warehouse: %r", dirname)
        self.move_dir(temp_path, warehouse_path)

    def move_dir(self, fromdir, todir):
        logger.info("Moving file: src=%r dest=%r", fromdir, todir)
        self.hdfs.rename_file_dir(fromdir, todir)

    #def check_job_is_running(self, dirname):
    #    """curl -q 'https://advana-jenkins.private.massmutual.com/job/plinkr-main/api/json?pretty=true&depth=2&tree=builds\[builtOn,changeSet,duration,timestamp,id,building,actions\[causes\[userId\]\]\]'"""
    #    url = "%s/view/squark-running/api/json" % os.environ['JENKINS_URL']
    #    data = requests.get(url).json()
    #    for job in data['jobs']:
    #        if dirname in job["name"]:
    #            return True

    def rotate_dataset(self, dirname):
        logger.info("Rotating dataset: %r", dirname)
        #if self.check_job_is_running(dirname):
        #    logger.warning("Not rotating dataset (build in progress): %s", dirname)
        #    return
        self.archive_dir(dirname)
        self.deploy_dir(dirname)

    def rotate_all_datasets(self):
        logger.info("Rotating all datasets in %r", SQUARK_TEMP)
        for filestatus in self.hdfs.list_dir(SQUARK_TEMP)['FileStatuses']['FileStatus']:
            if filestatus['type'] == 'FILE':
                logger.warning("Not a directory, so skipping it: %r", filestatus)
                continue
            self.rotate_dataset(filestatus['pathSuffix'])
        logger.info("All datasets have been rotated.")


if __name__ == '__main__':
    client = JenkinsClient()
    #client.deactivate_squark_jobs()
    #client.join_running_squark_jobs()
    #client.rotate_all_datasets()
    #client.reactivate_squark_jobs()

    dirname = sys.argv[1]
    client.rotate_dataset(dirname)
