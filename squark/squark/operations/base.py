import functools
import contextlib
from types import *

from squark.table import Table, get_table
from squark.location import Location, get_location
from squark.utils.common import CachedAttr


def funcify_operation(Cls):
    @functools.wraps(Cls.__init__)
    def wrapped(*args, **kwargs):
        return Cls(*args, **kwargs).run()
    return wrapped


class Operation:

    # -------------------------------------------------------------------------
    # Helpers for args/options common to all operations.
    # -------------------------------------------------------------------------
    @property
    def nocommit(self):
        return getattr(self, '_nocommit', False)

    @property
    def src(self):
        return self.src_location

    @src.setter
    def src(self, value):
        self._src_raw = value
        # Set src_location
        if isinstance(value, str):
            self.src_location = get_location(value)
        elif isinstance(value, Location):
            self.src_location = value
        elif isinstance(value, Table):
            self.src_location = value.location
        else:
            raise TypeError('Unknown src type: %r' % value)

        # Set src_table
        self.src_table = get_table(self.src_location)

    # -------------------------------------------------------------------------
    # Interface for running tasks.
    # -------------------------------------------------------------------------
    def run(self):
        self.validate()
        if self.src.is_glob():
            self._run_many()
        else:
            self._run_one(self.src_location)

    def _run_one(self, location: Location = None):
        location = location or self.src
        with self.maybe_transaction():
            self.run_one(location)

    def run_one(self, location: Location = None):
        raise NotImplementedError()

    def run_many(self):
        for loc in self.src.backend.expand_glob(self.src):
            self.run_one(loc)

    def _run_many(self):
        nocommit = self.nocommit
        with self.maybe_transaction(nocommit):
            self.run_many()

    def get_backend(self):
        return self.src_table.backend

    @contextlib.contextmanager
    def maybe_transaction(self):
        with self.get_backend().maybe_transaction(self.nocommit):
            yield

    # -------------------------------------------------------------------------
    # Sanity checks on this task invocation.
    # -------------------------------------------------------------------------
    def validate(self):
        '''Subclasses can define specific validation here.
        '''

    def _validate(self):
        self.check_backend_support()
        self.validate()

    def check_backend_support(self):
        '''Verify that the backend supports this operation.
        XXX: todo
        '''


class UnaryOperation(Operation):

    def run_one(self, location: Location = None):
        raise NotImplementedError()


class BinaryOperation(Operation):

    @property
    def dest(self):
        return self.dest_location

    @dest.setter
    def dest(self, value):
        self._dest_raw = value
        # Set src_location
        if isinstance(value, str):
            self.dest_location = get_location(value)
        elif isinstance(value, Location):
            self.dest_location = value
        elif isinstance(value, Table):
            self.dest_location = value.location
        else:
            raise TypeError('Unknown dest type: %r' % value)

        # Set src_table
        self.dest_table = get_table(self.dest_location)

    def run_one(self, location: Location = None):
        raise NotImplementedError()


class SharedBackendOperation(BinaryOperation):
    pass


class MultiBackendOperation(BinaryOperation):
    pass
