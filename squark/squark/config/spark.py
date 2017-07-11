from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import HiveContext

from squark.base import ConfigType
from squark.utils.common import CachedAttr


class Spark(ConfigType):

    @CachedAttr
    def conf(self):
        conf = SparkConf()
        for key, value in self.cfg['SparkConf'].items():
            if key == 'spark.executor.extraClassPath':
                paths = value.split(',')
                paths = [os.path.abspath(p) for p in paths]
                value = ','.join(paths)
            conf.set(key, value)
        return conf

    @CachedAttr
    def context(self):
        '''Set up the SparkContext.
        '''
        kwargs = {}
        if 'SparkContext' in self.cfg:
            kwargs = dict(self.cfg['SparkContext'])
        self.logger.info("Creating spark context")
        ctx = SparkContext(conf=self.conf, **kwargs)
        return ctx

    @CachedAttr
    def sql_context(self):
        '''Set up the SqlContext.
        '''
        kwargs = {}
        if 'HiveContext' in self.cfg:
            kwargs = dict(self.cfg['HiveContext'])
        self.logger.info("Creating Hive context")
        return HiveContext(self.context, **kwargs)

    @property
    def ctx(self):
        return self.context

    @property
    def sc(self):
        return self.context

    @property
    def sqlctx(self):
        return self.sql_context