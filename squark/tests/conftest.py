import os

import pytest
from py4jdbc.dbapi2 import Connection

import squark.backend
from squark.table import get_table
from squark.config.environment import Environment


@pytest.fixture(scope="session")
def env():
    return Environment()


@pytest.fixture(scope="session")
def fs():
    '''Shortcut for FileBackend().
    '''
    return squark.backend.FileBackend()


@pytest.fixture(scope="session")
def df1():
    '''Example dataframe 1.
    '''
    path = os.path.join(Environment().FIXTURES, 'df1')
    path = 'file://' + path
    return get_table(path)

@pytest.fixture(scope='session')
def derby():
    env = Environment()
    return env.sources.derby.connection


@pytest.fixture(scope='session')
def postgres():
    env = Environment()
    return env.sources.postgres.connection


@pytest.fixture(scope='session')
def pg1():
    env = Environment()
    return env.sources.pg1.connection


@pytest.fixture(scope='session')
def pg2():
    # Start a gateway.
    env = Environment()
    return env.sources.pg2.connection


@pytest.fixture(scope="session")
def jdbc():
    '''Shortcut for FileBackend().
    '''
    return squark.backend.JdbcBackend()


@pytest.fixture(scope='session')
def vertica():
    # Start a gateway.
    env = Environment()
    return env.sources.vertica


@pytest.fixture(scope='session')
def vertica_conn():
    # Start a gateway.
    env = Environment()
    return env.sources.vertica.connection
