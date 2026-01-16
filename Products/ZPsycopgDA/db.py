# ZPsycopgDA/db.py - query execution
#
# Copyright (C) 2004-2010 Federico Di Gregorio  <fog@debian.org>
#
# psycopg2 is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# psycopg2 is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
# License for more details.

# Import modules needed by _psycopg to allow tools like py2exe to do
# their work without bothering about the module dependencies.

from Shared.DC.ZRDB.TM import TM
from Shared.DC.ZRDB import dbi_db

import psycopg2
import psycopg
from psycopg2.extensions import INTEGER, LONGINTEGER, BOOLEAN, DATE, TIME
from psycopg2.extensions import register_type
from psycopg2 import NUMBER, STRING, ROWID, DATETIME
from psycopg2.pool import AbstractConnectionPool, ThreadedConnectionPool
from psycopg_pool import ConnectionPool
import threading

try:
    from Zope2.App.startup import RetryError, RetryDelayError
except ImportError:
    # Fallback declarations for graceful degradation,
    # Zope will not retry as intended!
    try:
        from Zope2.App.startup import ConflictError
    except ImportError:
        # With Zope4, this changed
        from ZODB.POSException import ConflictError
    RetryError = ConflictError
    RetryDelayError = ConflictError

from logging import getLogger
LOG = getLogger('ZPsycopgDA.db')


def get_thread_id():
    '''Global function to retrieve the current thread id.'''
    try:
        import thread
        threading = thread
    except ImportError:
        import threading
    return threading.get_ident()


# the DB object, managing all the real query work
class DB(TM, dbi_db.DB):

    _p_oid = _p_changed = _registered = None

    def __init__(self, dsn, tilevel, typecasts, enc='utf-8',
                 autocommit=False, readonlymode=False, physical_path='',
                 use_tpc=False):
        self.dsn = dsn
        self.tilevel = tilevel
        self.typecasts = typecasts
        if enc is None or enc == "":
            self.encoding = "utf-8"
        else:
            self.encoding = enc
        self.failures = 0
        self.calls = 0

        self.physical_path = physical_path

        # Patch JJ 2017-01-27: Add an autocommit feature which commits
        # every query in this instance
        self.autocommit = autocommit

        # Patch JJ 2017-09-26: Add read-only mode
        self.readonlymode = readonlymode

        # Patch JJ 2018-10-15: Use Two-Phase Commit
        self.use_tpc = use_tpc

        # Connectors with tainted transactions
        self.tainted = []

        # Connectors that have uncommited changes
        self.in_transaction = set()

        self.make_mappings()

        self._tls = threading.local()

        def configure(connection: psycopg.Connection):
            connection.autocommit = self.autocommit
            connection.read_only = self.readonlymode
            connection.isolation_level = self.tilevel

        self.pool = ConnectionPool(
            min_size=0, max_size=100, conninfo=self.dsn, configure=configure, name=self.physical_path)

    def getconn(self, init=True):
        if getattr(self._tls, "conn", None) is None:
            self._tls.conn = self.pool.getconn()
        return self._tls.conn

    def putconn(self, close=False):
        conn = self.getconn()
        if close:
            conn.close()
        self._tls.conn = None
        return self.pool.putconn(conn)

    def getcursor(self):
        conn = self.getconn()
        try:
            cursor = conn.cursor()
        except psycopg.InterfaceError:
            # Connection is broken. Put away, then raise.
            self.putconn(close=True)
            raise
        return cursor

    def _commit(self, put_connection=False):
        conn = self.getconn(False)
        conn.commit()
        if put_connection:
            self.putconn()

    # Two-Phase Commit (TPC) is needed in order to be able to cope with
    # errors during "COMMIT".
    # dbapi and psycopg2 have TPC support.
    # The method should be included into the Zope Transaction Manager so:
    # - When calling _register(), also do a dbconn2.Connection.tpc_begin().
    # - implement commit() to perform the first phase (tpc_prepare())
    # - implement _finish() to perform the second phase (tpc_commit())
    def xid(self):
        '''generate a valid transaction ID for two-phase commit.'''
        conn = self.getconn(False)
        xid = conn.xid(1, str(get_thread_id()), self.physical_path)
        return xid

    def _begin(self):
        conn = self.getconn(False)
        if self.use_tpc:
            xid = self.xid()
            conn.tpc_begin(xid)

    def commit(self, *ignored):
        conn = self.getconn()
        if conn in self.tainted:
            return
        if self.use_tpc:
            try:
                conn = self.getconn(False)
                conn.tpc_prepare()
            except psycopg2.Error as error:
                self.handle_retry(error)
                raise error

    def _finish(self, *ignored):
        conn = self.getconn(False)
        if conn in self.tainted:
            self._abort()
            return
        if self.use_tpc:
            conn.tpc_commit()
        else:
            self._commit(put_connection=True)
        self.in_transaction.discard(conn)

    def _abort(self, *ignored):
        # In cases where the _abort() occurs because the connection to the
        # database failed, getconn() will fail also.
        try:
            conn = self.getconn(False)
        except psycopg.Error:
            LOG.error('getconn() failed during abort.')
            return

        try:
            if self.use_tpc:
                # TODO An error can occur if this connector
                # has not yet started a transaction. Maybe
                # possible to fix using tpc_abort() rather than
                # abort().
                try:
                    conn.tpc_rollback()
                except psycopg.ProgrammingError:
                    pass
            else:
                conn.rollback()
        except psycopg.InterfaceError:
            LOG.error('Rollback failed, just closing connection.')
        if conn in self.tainted:
            self.tainted.remove(conn)
        self.putconn()

    def open(self):
        pass

    def close(self):
        pass

    def sortKey(self):
        return '1'

    def make_mappings(self):
        """Generate the mappings used later by self.convert_description()."""
        self.type_mappings = {}
        for t, s in [(INTEGER, 'i'), (LONGINTEGER, 'i'), (NUMBER, 'n'),
                     (BOOLEAN, 'n'), (ROWID, 'i'),
                     (DATETIME, 'd'), (DATE, 'd'), (TIME, 'd')]:
            for v in t.values:
                self.type_mappings[v] = (t, s)

    def convert_description(self, desc, use_psycopg_types=False):
        """Convert DBAPI-2.0 description field to Zope format."""
        items = []
        for name, typ, width, ds, p, scale, null_ok in desc:
            m = self.type_mappings.get(typ, (STRING, 's'))
            items.append({
                'name': name,
                'type': use_psycopg_types and m[0] or m[1],
                'width': width,
                'precision': p,
                'scale': scale,
                'null': null_ok,
            })
        return items

    # tables and rows

    def tables(self, rdb=0, _care=('TABLE', 'VIEW')):
        self._register()
        c = self.getcursor()
        c.execute(
            "SELECT t.tablename AS NAME, 'TABLE' AS TYPE "
            "  FROM pg_tables t WHERE tableowner <> 'postgres' "
            "UNION SELECT v.viewname AS NAME, 'VIEW' AS TYPE "
            "  FROM pg_views v WHERE viewowner <> 'postgres' "
            "UNION SELECT t.tablename AS NAME, 'SYSTEM_TABLE\' AS TYPE "
            "  FROM pg_tables t WHERE tableowner = 'postgres' "
            "UNION SELECT v.viewname AS NAME, 'SYSTEM_TABLE' AS TYPE "
            "FROM pg_views v WHERE viewowner = 'postgres'")
        res = []
        for name, typ in c.fetchall():
            if typ in _care:
                res.append({'TABLE_NAME': name, 'TABLE_TYPE': typ})
        self.putconn()
        return res

    def columns(self, table_name):
        self._register()
        c = self.getcursor()
        try:
            c.execute('SELECT * FROM "%s" WHERE 1=0' % table_name)
        except Exception:
            return ()
        self.putconn()
        return self.convert_description(c.description, True)

    @staticmethod
    def split_error(error):
        '''
        Split error in name and value for inspection
        '''
        return (error.__class__.__name__, repr(error))

    @staticmethod
    def is_connection_error(error):
        '''
        Errors that only affect our connection and where an immediate retry
        should work.
        AdminShutdown sounds bad, but it might only be our connection that is
        affected. When reconnecting after a regular Retry we see if it
        acutually something serious, in which case we will get something like
        'the database is shutting down'. If it is only our connection, a simple
        reconnect will work.
        '''
        (name, value) = DB.split_error(error)
        return name in (
            'AdminShutdown',
            'OperationalError',
            'InterfaceError'
        ) and (
            'SSL SYSCALL error' in value or
            'server closed the connection' in value or
            'terminating connection due to administrator command' in value or
            'connection already closed' in value
        )

    @staticmethod
    def is_server_error(error):
        '''
        Errors that indicate that the database encounters problems. Retry only
        after a few seconds.
        '''
        (name, value) = DB.split_error(error)
        return (
            name == 'OperationalError' and (
                'could not connect to server' in value or
                'the database system is shutting down' in value or
                'the database system is starting up' in value or
                'SSL connection has been closed unexpectedly' in value
            )
        ) or (
            name == 'NotSupportedError' and (
                'cannot set transaction read-write mode' in value
            )
        )

    @staticmethod
    def is_serialization_error(error):
        '''
        Original retry eror in case of serialization failures.
        '''
        (name, value) = DB.split_error(error)
        return (
            name in ('TransactionRollbackError', 'SerializationFailure')
            and 'could not serialize' in value
        )

    @staticmethod
    def is_spurious_collision_error(error):
        '''
        Detect whether the error condition is spurious or transitional and
        should therefore be retried without delay.

        So far, only Deadlock errors fall in this category.
        '''
        (name, value) = DB.split_error(error)
        if name == 'DeadlockDetected':
            return True
        return False

    def handle_retry(self, error):
        '''Find out if an error deserves a retry.'''
        if self.is_serialization_error(error):
            raise RetryError from error

        if self.is_spurious_collision_error(error):
            raise RetryError from error

        connection_error = self.is_connection_error(error)
        server_error = self.is_server_error(error)

        if connection_error or server_error:
            name, value = self.split_error(error)
            LOG.exception(
                "Error on connection. Closing. ({}, {})".format(name, value)
            )
            self.getconn().close()

        if connection_error:
            raise RetryError from error
        if server_error:
            raise RetryDelayError from error

    # query execution
    def query(self, query_string, max_rows=None, query_data=None):
        self._register()
        self.calls = self.calls+1

        error = None
        for retry in range(2):
            try:
                return self.query_inner(query_string, max_rows, query_data)
            except Exception as err:
                error = err
                conn = self.getconn()
                # First query in transaction yields a connection error - try to
                # simply reconnect
                if ((conn not in self.in_transaction)
                        and self.is_connection_error(err)):
                    LOG.warning(
                        "Connection error on first query in transaction, "
                        "reconnecting."
                    )
                    self.putconn(close=True)
                    continue
                break

        # We only reach this if another error occured
        self.handle_retry(error)
        self._abort()

        # Taint this transaction
        LOG.warning('query() tainting: {} in {}'.format(
            conn, self.tainted))
        if conn not in self.tainted:
            self.tainted.append(conn)
        raise error

    def query_inner(self, query_string, max_rows=None, query_data=None):
        conn = self.getconn()
        if conn in self.tainted:
            raise ValueError("Query attempted on tainted transaction.")

        desc = ()
        res = []
        nselects = 0

        c = self.getcursor()

        for qs in [x for x in query_string.split('\0') if x]:
            # LOG.info("Trying to execute statement %s" % qs)
            if query_data:
                c.execute(qs, query_data)
            else:
                c.execute(qs)

            if self.autocommit:
                # LOG.info('Autocommitting.')
                self._commit()

            if c.description is not None:
                nselects += 1
                if c.description != desc and nselects > 1:
                    raise psycopg.ProgrammingError(
                        'multiple selects in single query not allowed')
                if max_rows:
                    res = c.fetchmany(max_rows)
                    # JJ 2017-07-20: Danger ahead. We might
                    # have many more rows in the database,
                    # which are truncated by max_rows. In that
                    # case, we should be able to react, by
                    # raising or logging.
                    if len(res) == max_rows:
                        try:
                            overshoot_result = c.fetchone()
                        except Exception:
                            overshoot_result = None
                        if overshoot_result:
                            assert False, (
                                "This query has returned more than "
                                "max_rows results. Please raise "
                                "max_rows or limit in SQL.")

                else:
                    res = c.fetchall()
                desc = c.description

        self.failures = 0

        self.in_transaction.add(conn)
        return self.convert_description(desc), res
