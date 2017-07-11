import click

from squark.location import get_location
from squark.table import get_table


class LocationParamType(click.ParamType):
    name = 'location'

    def convert(self, value, param, ctx):
        try:
            return get_location(value)
        except ValueError:
            self.fail('%s is not a valid location' % value, param, ctx)

location = LocationParamType()


class TableParamType(click.ParamType):
    name = 'table'

    def convert(self, value, param, ctx):
        try:
            return get_table(value)
        except ValueError:
            self.fail('%s is not a valid table location' % value, param, ctx)

table = TableParamType()