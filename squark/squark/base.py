import re
import os
import logging

from squark.utils.common import CachedAttr


class ConfigType:

    def __init__(self, cfg):
        self.cfg = cfg
        self.logger = logging.getLogger('squark')

    # Much easier to implement this on a non-dict than
    # the other way around...
    __getitem__ = object.__getattribute__


class LocationType:
    pass


class TableType:
    pass


class BackendType:
    pass


class OperationType:
    '''An operation on a dataframe. Operation provides an abstraction
    over tables and backends, and it agnostic about this things in the
    way it implements copy, mv, rm, rotate etc.
    '''
    def __init__(self, env):
        self.env = env

    def __call__(self, *args, **kwargs):
        raise NotImplementedError()


class ReportType:
    '''Operations return reports that tell you stuff.
    '''

