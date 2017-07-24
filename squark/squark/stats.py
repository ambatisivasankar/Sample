import copy
import base64
import pickle
from collections import defaultdict

import six

import pyspark.sql.functions as F
from pyspark.sql import types as sqltypes


class StatsCalculator:
    '''Given configuration data like:

        ---
    Expected configuration format:
    {
        'dataframe: ['count','count_distinct','freq'],
        'profiles':
            { 
                'profileName': ['list','of','function','names']
            },
        'fields_types:
            { 
                'NumericType': 'profileName',
                'StringType': 'profileName'
            }
        'fields':
            {
                'field1': 'profileName',
                'field2': 'profileName'
            }
    }

    Format expects two main keys, profiles and either field_types or fields.
        - Each field type should be of a type found in `pyspark.sql.types`. 
        - Each field should be a column in the dataframe.
        - Each function should be a function that is available in `pyspark.sql.functions`.
        - All values for either the fields or the field types should be a profile name specified.

    Computes these stats for each column of the specified
    `types` and returns the results in a dict, keyed by
    column name.
    '''
    top_n = 50

    def __init__(self, df, config):
        self.df = df
        self.config = config
        self.result = dict(fields=defaultdict(dict))
        self.expressions = self.exprs = []

    def stats(self):
        '''Returns a mapping of schema fields to stats dicts.
        '''
        df_cfg = self.config.get('dataframe', [])
        self.result['count'] = self.count = self.df.count()
        if 'count_distinct' in df_cfg:
            self.result['count_distinct'] = self.df.distinct().count()
        if 'freq' in df_cfg:
            self.result['freq'] = self.df.freqItems(cols=self.df.columns).take(50)

        field_types = {}
        print("LOADING FIELD_TYPES:")
        for k, v in self.config.get('field_types', {}).items():
            print(" - field type: %s    - profile: %s"%(k,v))
            field_types[getattr(sqltypes, k)] = v

        print("FIELD TYPES:",field_types)
        agg = []
        for field in self.df.schema.fields:
            print("WORKING ON FIELD: %s"%(field.name))
            cfg_fields = self.config.get('fields', [])
            if field.name in cfg_fields:
                profileName = cfg_fields[field.name]
                self.process_field(field, profileName)

            if field_types:
                print(" -- PROCESSING FIELD_TYPES:")
                for tipe, profileName in field_types.items():
                    print("--- field.dataType: %s  -- type: %s"%(field.dataType, tipe))
                    print(" --- isinstance(field.dataType, tipe): ",isinstance(field.dataType, tipe))
                    if isinstance(field.dataType, tipe):
                        if profileName is None:
                            continue
                        self.process_field(field, profileName)
        if self.exprs:
            agg = self.df.agg(*self.exprs)
        return self.add_results(agg)

    def add_results(self, agg):
        if not agg:
            return dict(self.result)
        colnames = list(sorted(self.df.columns, reverse=True, key=len))
        for key, val in agg.first().asDict().items():
            if val is None:
                continue
            for colname in colnames:
                if key.startswith(colname):
                   key = key.replace(colname, '', 1).lstrip('_')
                   self.result['fields'][colname][key] = val
        return dict(self.result)

    def process_field(self, field, profileName):
        print(" ----- IN PROCESS FIELD: ", isinstance(profileName, six.string_types))
        if isinstance(profileName, six.string_types):
            profile = self.config.get('profiles', {})
            funcs = profile.get(profileName, [])
            print(" ----- FUNCS:", funcs)
            for func in funcs:
                self.add_expr(field, func)

    def add_expr(self, field, funcname):
        # Check for a custom handler.
        methname = 'add_expr_' + funcname
        method = getattr(self, methname, None)
        if method is not None:
            return method(field)

        # Or do the default.
        col = F.col(field.name)
        if isinstance(field.dataType, sqltypes.StringType):
            # If it's a string column, all stats will be computed
            # using the length of values in the column.
            col = F.length(col)
        func = getattr(F, funcname)
        expr = func(col).alias('%s_%s' % (field.name, funcname))
        self.exprs.append(expr)

    def add_expr_percent_null(self, field):
        '''Returns an expression to count the percent of null
        values in this column.
        '''
        col = F.col(field.name)
        expr = F.sum(F.isnull(col).cast("integer")) / self.count
        expr = expr.alias('%s_percent_null' % field.name)
        self.exprs.append(expr)

    def add_expr_count_null(self, field):
        '''Returns an expression to count the percent of null
        values in this column.
        '''
        col = F.col(field.name)
        expr = F.sum(F.isnull(col).cast("integer"))
        expr = expr.alias('%s_count_null' % field.name)
        self.exprs.append(expr)

def get_stats(df, config):
    return StatsCalculator(df, config).stats()


def add_stats_to_table(table, stats):
    df = table.df
    schema = copy.copy(df.schema)
    for field in schema.fields:
        field_stats = stats['fields'].get(field.name)
        if field_stats is None:
            continue
        string = base64.b64encode(pickle.dumps(field_stats)).decode('ascii')
        field.metadata['stats'] = string
    table.df = table.env.sqlctx.createDataFrame(df.rdd, schema)
    return table


def get_stats_from_table(table):
    stats = {}
    for field in table.df.schema.fields:
        field_stats = field.metadata.get('stats')
        if field_stats is None:
            continue
        field_stats = pickle.loads(base64.b64decode(field_stats))
        stats[field.name] = field_stats
    return stats

