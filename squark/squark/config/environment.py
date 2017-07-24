import os
import signal
import logging
import logging.config
from configparser import ConfigParser, ExtendedInterpolation
from os.path import abspath, dirname, join

import dirigible

import squark
from squark.config.spark import Spark
from squark.config.sources import Sources
from squark.config.commands import Commands

from squark.utils.common import CachedAttr, resolve_name, Singleton

from squark.exceptions import ConfigError, ConfigNotFoundError


class LoggingAlias:

    def __init__(self, loglevel):
        self.loglevel = loglevel

    def __get__(self, inst, cls):
        return getattr(inst.logger, self.loglevel)


class Environment(dirigible.Config):
    CODE = abspath(squark.__name__)
    CONFIG = join(CODE, 'config')
    CONFIG_DEFAULTS = join(CONFIG, 'defaults')
    ROOT = dirname(CODE)
    TEMPLATES = join(CODE, 'templates')
    SQL_TEMPLATES = join(TEMPLATES, 'sql')
    TESTS = join(ROOT, 'tests')
    FIXTURES = join(TESTS, 'fixtures')

    debug = LoggingAlias('debug')
    info = LoggingAlias('info')
    warn = LoggingAlias('warn')
    warning = LoggingAlias('warning')
    error = LoggingAlias('error')
    critical = LoggingAlias('critical')
    fatal = LoggingAlias('fatal')

    def __init__(self, handle_signals=True, require_ctx_manager=True):
        self._in_ctx_manager = False
        self._require_ctx_manager = require_ctx_manager
        self._handle_signals = handle_signals
        # Database connections are tracked here so that can be
        # closed later.
        self._connections = []
        super().__init__(appname='squark', defaults_dir=self.CONFIG_DEFAULTS)

    # -----------------------------------------------------------------------
    # Low-level stuff to ensure connections get closed.
    # -----------------------------------------------------------------------
    def _shutdown_handler(self, signal, frame):
        self._teardown()

    def _setup(self):
        shutdown_signals = (
            signal.SIGINT, signal.SIGTERM,
            signal.SIGHUP, signal.SIGQUIT,
            signal.SIGABRT)
        self.info('Setting shutdown signal handlers.')
        for sig in shutdown_signals:
            handler = signal.getsignal(sig)
            if handler is self._shutdown_handler:
                continue
            elif handler not in [0, signal.default_int_handler]:
                def handler(*args, **kwargs):
                    handler(*args, **kwargs)
                    signal.default_int_handler(*args, **kwargs)
            else:
                handler = self._shutdown_handler
            signal.signal(sig, handler)

    def __enter__(self):
        self._in_ctx_manager = True
        return self

    def __exit__(self, *args):
        self._teardown()

    def _teardown(self):
        self.info('Closing any open database connections.')
        for source in self._connections:
            if not source.connection._closed:
                msg = 'Closing connection for source: %r'
                source.connection.close()
            else:
                msg = 'Connection for source already closed: %r'
            self.info(msg, source)

    @CachedAttr
    def logger(self):
        if getattr(self, '_log_config', None) is None:
            self.init_logging()
        self.logger = logging.getLogger('squark')
        return self.logger

    def init_logging(self, loglevel=None, color=True):
        # Log level.
        if loglevel is None:
            loglevel = os.getenv('SQUARK_LOG_LEVEL', 'INFO')
        # Whether to log with terminal colors.
        if color is None:
            color = os.getenv('SQUARK_LOG_COLOR', True)
        if color:
            handler = 'squark.utils.ansistrm.ColorizingStreamHandler'
        else:
            handler = 'logging.StreamHandler'

        log_config = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'standard': {
                    'format': "%(asctime)s %(levelname)s %(module)s::%(funcName)s: %(message)s",
                    'datefmt': '%H:%M:%S'
                }
            },
            'handlers': {
                'default': {
                    'level': loglevel,
                    'class': handler,
                    'formatter': 'standard'},
            },
            'loggers': {
                'squark': {
                    'handlers': ['default'],
                    'level': 'DEBUG',
                    'propagate': True
                },
            },
        }
        logging.config.dictConfig(log_config)
        self._log_config = log_config

    # ------------------------------------------------------------------------
    # Config-handling utils.
    # ------------------------------------------------------------------------
    @CachedAttr
    def cfg(self):
        return self.load_config_filename('squark.cfg')

    @CachedAttr
    def commands(self):
        cfg = self.load_config_filename('commands.cfg')
        return Commands(cfg)

    @CachedAttr
    def sources(self):
        source_cfg = self.load_config_filename('sources.cfg')
        return Sources(source_cfg, env=self)

    @CachedAttr
    def stats_config(self):
        return self.load_config_filename('stats.yml')

    # ------------------------------------------------------------------------
    # Spark shortcuts
    # ------------------------------------------------------------------------
    @CachedAttr
    def spark(self):
        cfg = self.load_config_filename('squark.cfg')
        return Spark(cfg)

    @property
    def spark_context(self):
        return self.spark.context

    @property
    def spark_sql_context(self):
        return self.spark.sql_context

    sc = ctx = spark_context
    sqlctx = spark_sql_context
