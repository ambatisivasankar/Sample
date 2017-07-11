#!/usr/bin/env python

import logging
from time import sleep

from squark.backend import JdbcBackend
from squark.location import get_location
from squark.config.environment import Environment
from py4jdbc.exceptions.sqlstate import ConnectionError


logger = logging.getLogger()
logging.basicConfig()


def main():
    # Start a gateway.
    vertica = Environment().sources.vertica

    alive = False
    tries = 0
    while not alive:
        if tries == 3:
            raise SystemExit("Couldn't connect to vertica")
        try:
            cur = vertica.conn.cursor()
            cur.execute("select 1 as squarktest")
        except ConnectionError as exc:
            logger.exception(exc)
            logger.info('Sleeping 5 seconds.')
            sleep(5)
            tries += 1
        else:
            rs = cur.fetchall()
            alive = True


if __name__ == '__main__':
    main()
