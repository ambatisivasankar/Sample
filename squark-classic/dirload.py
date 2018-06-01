import os
import sys
import subprocess
import itertools
from collections import defaultdict
import tempfile
import logging
import boto3
import glob
import time
import re

from pywebhdfs.webhdfs import PyWebHdfsClient
import squark.config.environment
import utils

squarkenv = squark.config.environment.Environment()

try:
    VERTICA_CONNECTION_ID = os.environ['VERTICA_CONNECTION_ID']
except:
    VERTICA_CONNECTION_ID = "vertica_dev"

MAX_CONNS = int(os.getenv('VERTICA_PARALLELISM', 10))

PROJECT_ID = os.environ.get('PROJECT_ID')
SQUARK_TYPE = os.environ.get('SQUARK_TYPE')
LOAD_FROM_AWS = os.environ.get('LOAD_FROM_AWS')
LOAD_FROM_HDFS = os.environ.get('LOAD_FROM_HDFS')
S3_FUSE_LOCATION = os.environ.get('S3_FUSE_LOCATION','/mnt/s3/')
TABLE_NUM_RETRY = int(os.environ.get('SQUARK_NUM_RETRY','1'))
S3_CONNECTION_ID = os.environ.get('S3_CONNECTION_ID')

JENKINS_URL = os.environ.get('JENKINS_URL', '')
JOB_NAME = os.environ.get('JOB_NAME', '')
BUILD_NUMBER = os.environ.get('BUILD_NUMBER', '-1')
SKIP_UNIQUE_ID_CHECK = os.environ.get('SKIP_UNIQUE_ID_CHECK', '').lower() in ['1', 'true', 'yes']

INCLUDE_TABLES = os.environ.get('INCLUDE_TABLES')
if INCLUDE_TABLES is not None:
    INCLUDE_TABLES = [s.strip() for s in INCLUDE_TABLES.split(',') if s]

EXCLUDE_TABLES = os.environ.get('EXCLUDE_TABLES', [])
if EXCLUDE_TABLES:
    EXCLUDE_TABLES = [s.strip() for s in EXCLUDE_TABLES.split(',') if s]

if LOAD_FROM_AWS:
    aws = squarkenv.sources[S3_CONNECTION_ID]
    AWS_ACCESS_KEY_ID = aws.cfg['access_key_id']
    AWS_SECRET_ACCESS_KEY = aws.cfg['secret_access_key']
    SQUARK_BUCKET = os.environ['SQUARK_BUCKET']
    if SQUARK_BUCKET.lower() in ['squark','squark-dsprd']:
        raise TypeError('Invalid bucket specified: {}'.format(SQUARK_BUCKET))
    #SQUARK_BUCKET='squark'
    #vertica_aws_conn = squarkenv.sources['vertica_aws'].conn
if LOAD_FROM_HDFS:
    HDFS_HOST = os.environ['HDFS_HOST']
    HDFS_PORT = os.environ['HDFS_PORT']
    HDFS_USER = os.environ['HDFS_USER']

logging.basicConfig(level=logging.DEBUG)

def get_s3_urls(project_id):
    session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name='us-east-1')
    client = session.client('s3')
    prefix = '{SQUARK_TYPE}/{PROJECT_ID}/'.format(
                SQUARK_TYPE=SQUARK_TYPE,
                PROJECT_ID=project_id
                )
    tmp_paths = client.list_objects(Bucket=SQUARK_BUCKET, Prefix=prefix)
    paths = [x['Key'] for x in tmp_paths['Contents']]
    while tmp_paths['IsTruncated']:
        nextMarker = paths[-1]
        tmp_paths = client.list_objects(Bucket=SQUARK_BUCKET, Prefix=prefix, Marker=nextMarker)
        paths.extend([x['Key'] for x in tmp_paths['Contents']])

    print('--- Total pulled paths: %i    --- Total set of pulled paths: %i'%(len(paths), len(set(paths))))
    all_orcs = [x for x in paths if glob.re.search('.*\.orc/.*\.orc', x)]
    urls = defaultdict(list)
    for orc_file in all_orcs:
        tablename = orc_file.replace(prefix,'').strip('/').split('/')[0]

        if INCLUDE_TABLES is not None:
            if tablename not in INCLUDE_TABLES:
                print('*******SKIPPING NOT INCLUDED TABLE: %r' % tablename)
                continue

        if EXCLUDE_TABLES is not None:
            if tablename in EXCLUDE_TABLES:
                print('*******SKIPPING EXCLUDE_TABLES TABLE: %r' % tablename)
                continue

        urls[tablename].append(orc_file)
    return urls

def get_urls(dirname):
    urls = defaultdict(list)
    hdfs_host = HDFS_HOST
    hdfs_port = HDFS_PORT
    hdfs_user = HDFS_USER
    hdfs = PyWebHdfsClient(host=hdfs_host, port=hdfs_port, user_name=hdfs_user)
    for child in hdfs.list_dir(dirname)['FileStatuses']['FileStatus']:
        if child['pathSuffix'].startswith('_'):
            continue
        table_dir = os.path.join(dirname, child['pathSuffix'])
        for orcfile in hdfs.list_dir(table_dir)['FileStatuses']['FileStatus']:
            if orcfile['pathSuffix'].startswith('_'):
                continue
            url = "'webhdfs://%s:%s%s/%s'" % (hdfs_host, hdfs_port, table_dir, orcfile['pathSuffix'])
            urls[child['pathSuffix']].append(url)
    return urls


def do_s3_copyfrom(schema_name, table_name, table_prefix, urls):
    urls = urls[:]
    curr_retry = 0
    while urls:
        _urls = []
        for i in range(MAX_CONNS):
            try:
                _urls.append(urls.pop(0))
            except IndexError:
                break
        tmpl = "copy %s.%s from %s on any node orc direct;"
        table_name = table_prefix + table_name
        sql = tmpl % (schema_name, table_name, ',\n'.join(["'%s'"%(os.path.join(S3_FUSE_LOCATION, x)) for x in _urls]))
        #sql = tmpl % (schema_name, table_name, ',\n'.join([os.path.join(S3_FUSE_LOCATION, x) for x in _urls]))
        logging.info("sql: %r", sql)
        vertica_conn = squarkenv.sources[VERTICA_CONNECTION_ID].conn
        logging.info("---- Launching s3 copy command...")
        # Add retries for loading data from s3
        # curr_retry = 0
        retry_bool = True
        print('----------------------------')
        while retry_bool and curr_retry < TABLE_NUM_RETRY:
            print('Attempting to load table {table}: [Attempt {curr}/{tot}]'.format(
                    table=table_name, curr=curr_retry+1, tot=TABLE_NUM_RETRY))
            try:
                cursor = vertica_conn.cursor()
                cursor.execute(sql)
                cursor.close()
                retry_bool = False
            except Exception as e:
                print('!! -- An Error occurred while trying to load -- waiting 5 seconds to retry!')
                print(str(e))
                curr_retry += 1
                time.sleep(5)
        if retry_bool:
            print('ERROR!! Number of allowed retries exceeded!! Exiting')
            raise
        print('Load Successful...')
        print('----------------------------')

    return curr_retry + 1

def do_copyfrom(schema_name, table_name, table_prefix, urls):
    urls = urls[:]
    while urls:
        _urls = []
        for i in range(MAX_CONNS):
            try:
                _urls.append(urls.pop(0))
            except IndexError:
                break
        tmpl = "copy %s.%s from %s on any node orc direct;"
        table_name = table_prefix + table_name
        sql = tmpl % (schema_name, table_name, ',\n'.join(_urls))
        logging.info("sql: %r", sql)
        vertica_conn = squarkenv.sources[VERTICA_CONNECTION_ID].conn
        cursor = vertica_conn.cursor()
        logging.info("---- Launching copy command...")
        cursor.execute(sql)
        cursor.close()

def update_squark_load_timings(project_id, table_name, time_taken, attempt_count, source, total_table_count):
    jenkins_name = JENKINS_URL.split('.')[0].split('/')[-1]
    vertica_conn = squarkenv.sources[VERTICA_CONNECTION_ID].conn
    utils.send_load_timing_to_vertica(vertica_conn, jenkins_name, JOB_NAME, BUILD_NUMBER, project_id, table_name,
                                      time_taken, attempt_count, source, total_table_count)

def main():
    schema_name = sys.argv[1]
    dirname = sys.argv[2]
    try:
        table_prefix = sys.argv[3]
    except IndexError:
        table_prefix = ''
    if LOAD_FROM_AWS:
        aws_urls = get_s3_urls(PROJECT_ID)
        total_table_count = len(aws_urls.keys())
        print('DEBUG: S3 .orc url listing, sorted: {}'.format(sorted(aws_urls.items())))
        # a "part" file, e.g. part-00000-c4492a53-615d-4787-b284-96f6848c0aee-c000.snappy.orc
        p = re.compile('part-?\d{5}-?(\w{8}-?\w{4}-?\w{4}-?\w{4}-?\w{12}-?\w{4})')
        # sort by table to match all_tables processing -> last written table will be last loaded, better for S3 store
        for table_name, aws_urls in sorted(aws_urls.items()):
            unique_ids = set(p.findall('|'.join(aws_urls)))
            print('table {}, unique id values in part files: {}'.format(table_name, ', '.join(unique_ids)))
            if not SKIP_UNIQUE_ID_CHECK:
                if len(unique_ids) > 1:
                    raise ValueError(
                        'S3 folder for table {} contains part files from multiple operations, unique ids: {}'.format(
                            table_name, unique_ids))
            print('XXX: Loading S3 %s (%d files)' % (table_name, len(aws_urls)))
            s1 = time.time()
            num_attempts = do_s3_copyfrom(schema_name, table_name, table_prefix, aws_urls)
            table_time = time.time() - s1
            # admin table will be updated after each table is loaded to vertica, i.e. even if full job later fails
            update_squark_load_timings(project_id=PROJECT_ID, table_name=table_name, time_taken=table_time,
                                       attempt_count=num_attempts, source='s3', total_table_count=total_table_count)

    if LOAD_FROM_HDFS:
        urls = get_urls(dirname)
        total_table_count = len(urls.keys())
        items = list(urls.items())
        items.sort(key=lambda item: len(item[1]), reverse=True)
        for table_name, urls in urls.items():
            print('XXX: Loading %s (%d files)' % (table_name, len(urls)))
            s1 = time.time()
            do_copyfrom(schema_name, table_name, table_prefix, urls)
            table_time = time.time() - s1
            update_squark_load_timings(project_id=PROJECT_ID, table_name=table_name, time_taken=table_time,
                                       attempt_count=1, source='hdfs', total_table_count=total_table_count)


if __name__ == '__main__':
    main()

