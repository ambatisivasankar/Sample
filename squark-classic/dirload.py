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
import functools

from pywebhdfs.webhdfs import PyWebHdfsClient
import squark.config.environment

print_now = functools.partial(print, flush=True)
squarkenv = squark.config.environment.Environment()

try:
    VERTICA_CONNECTION_ID = os.environ['VERTICA_CONNECTION_ID']
except:
    VERTICA_CONNECTION_ID = "vertica_dev"

MAX_CONNS = int(os.getenv('VERTICA_PARALLELISM', 10))
HDFS_HOST = os.environ['HDFS_HOST']
HDFS_PORT = os.environ['HDFS_PORT']
HDFS_USER = os.environ['HDFS_USER']

PROJECT_ID = os.environ.get('PROJECT_ID')
SQUARK_TYPE = os.environ.get('SQUARK_TYPE')
LOAD_FROM_AWS = os.environ.get('LOAD_FROM_AWS')
LOAD_FROM_HDFS = os.environ.get('LOAD_FROM_HDFS')
S3_FUSE_LOCATION = os.environ.get('S3_FUSE_LOCATION','/mnt/s3/')
TABLE_NUM_RETRY = int(os.environ.get('SQUARK_NUM_RETRY','1'))
S3_CONNECTION_ID = os.environ.get('S3_CONNECTION_ID')

if LOAD_FROM_AWS:
    aws = squarkenv.sources[S3_CONNECTION_ID]
    AWS_ACCESS_KEY_ID = aws.cfg['access_key_id']
    AWS_SECRET_ACCESS_KEY = aws.cfg['secret_access_key']
    SQUARK_BUCKET = os.environ.get('SQUARK_BUCKET','squark')
    #SQUARK_BUCKET='squark'
    #vertica_aws_conn = squarkenv.sources['vertica_aws'].conn
    

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
    all_orcs = [x for x in paths if glob.re.search('.*\.orc/.*\.orc', x)]
    urls = defaultdict(list)
    for orc_file in all_orcs:
        tablename = orc_file.replace(prefix,'').strip('/').split('/')[0]
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
        curr_retry = 0
        retry_bool = True
        print_now('----------------------------')
        while retry_bool and curr_retry < TABLE_NUM_RETRY:
            print_now('Attempting to load table {table}: [Attempt {curr}/{tot}]'.format(
                    table=table_name, curr=curr_retry+1, tot=TABLE_NUM_RETRY))
            try:
                cursor = vertica_conn.cursor()
                cursor.execute(sql)
                cursor.close()
                retry_bool = False
            except Exception as e:
                print_now('!! -- An Error occurred while trying to load -- waiting 5 seconds to retry!')
                print_now(str(e))
                curr_retry += 1
                time.sleep(5)
        if retry_bool:
            print_now('ERROR!! Number of allowed retries exceeded!! Exiting')
            raise
        print_now('Load Successful...')
        print_now('----------------------------')

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

def main():
    schema_name = sys.argv[1]
    dirname = sys.argv[2]
    try:
        table_prefix = sys.argv[3]
    except IndexError:
        table_prefix = ''
    if LOAD_FROM_AWS:
        aws_urls = get_s3_urls(PROJECT_ID)
        items = list(aws_urls.items())
        print_now('OCG MAX_CONNS: {}'.format(MAX_CONNS))
        print_now('OCG items before: {}'.format(items))
        #items.sort(key=lambda item: len(item[1]), reverse=True)
        #print_now('OCG items after sorting: {}'.format(items))
        # below was using the "raw" unsorted aws_urls.items() originally
        # add in a sorted so it does a simple sort on table name, to match all_tables processing
        # - that way the first-written table will be read first also, reduce eventual-consistency issues?
        for table_name, aws_urls in sorted(aws_urls.items()):
            print_now('XXX: Loading S3 %s (%d files)' % (table_name, len(aws_urls)))
            do_s3_copyfrom(schema_name, table_name, table_prefix, aws_urls)
    if LOAD_FROM_HDFS:
        urls = get_urls(dirname)
        items = list(urls.items())
        items.sort(key=lambda item: len(item[1]), reverse=True)
        for table_name, urls in urls.items():
            print_now('XXX: Loading %s (%d files)' % (table_name, len(urls)))
            do_copyfrom(schema_name, table_name, table_prefix, urls)


if __name__ == '__main__':
    main()

