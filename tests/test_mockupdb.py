#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Test MockupDB."""

import contextlib
import time
import sys

if sys.version_info[0] < 3:
    from io import BytesIO as StringIO
else:
    from io import StringIO

try:
    from queue import Queue
except ImportError:
    from Queue import Queue

from pymongo.errors import ConnectionFailure
from pymongo.topology_description import TOPOLOGY_TYPE
from pymongo import MongoClient, ReadPreference

from mockupdb import MockupDB, wait_until, OpReply, going, Future, go
from tests import unittest  # unittest2 on Python 2.6.


@contextlib.contextmanager
def capture_stderr():
    sio = StringIO()
    stderr, sys.stderr = sys.stderr, sio
    try:
        yield sio
    finally:
        sys.stderr = stderr
        sio.seek(0)


class TestGoing(unittest.TestCase):
    def test_nested_errors(self):
        def thrower():
            raise AssertionError("thrown")

        with capture_stderr() as stderr:
            with self.assertRaises(ZeroDivisionError):
                with going(thrower) as future:
                    1 / 0

        self.assertIn('error in going(', stderr.getvalue())
        self.assertIn('AssertionError: thrown', stderr.getvalue())

        # Future keeps raising.
        self.assertRaises(AssertionError, future)
        self.assertRaises(AssertionError, future)


class TestIsMasterFrequency(unittest.TestCase):
    def test_server_selection(self):
        primary, secondary, slow = servers = [MockupDB() for _ in range(3)]
        q = Queue()
        for server in servers:
            server.subscribe(q.put)
            server.run()
            self.addCleanup(server.stop)

        hosts = [server.address_string for server in servers]
        primary_reply = OpReply(ismaster=True, setName='rs', hosts=hosts)
        secondary_reply = OpReply(ismaster=False, secondary=True,
                                  setName='rs', hosts=hosts)

        primary.autoresponds('ismaster', primary_reply)
        secondary.autoresponds('ismaster', secondary_reply)

        def slow_ismaster(req):
            time.sleep(0.1)  # Sleep 100 ms.
            req.reply(secondary_reply)
            return True

        slow.autoresponds('ismaster', slow_ismaster)

        # Local threshold is 0.1 ms.
        client = MongoClient(primary.uri, replicaSet='rs', localThresholdMS=0.1)
        time.sleep(0.25)

        # Eventually finds slow secondary.
        self.assertIn(slow.address, client.secondaries)

        # Command sent to primary.
        future = go(client.db.command, 'hi')
        q.get().assert_matches('hi', server_port=primary.port).ok()
        future()

        # Command sent to secondary.
        db = client.get_database('db', read_preference=ReadPreference.SECONDARY)
        future = go(db.command, 'hi')
        request = q.get().assert_matches('hi', server_port=secondary.port)
        request.ok()
        future()


class TestNetworkDisconnectPrimary(unittest.TestCase):
    def test_network_disconnect_primary(self):
        # Application operation fails against primary. Test that topology
        # type changes from ReplicaSetWithPrimary to ReplicaSetNoPrimary.
        # http://bit.ly/1B5ttuL
        primary, secondary = servers = [MockupDB() for _ in range(2)]
        for server in servers:
            server.run()
            self.addCleanup(server.stop)

        hosts = [server.address_string for server in servers]
        primary_response = OpReply(ismaster=True, setName='rs', hosts=hosts)
        primary.autoresponds('ismaster', primary_response)
        secondary.autoresponds(
            'ismaster',
            ismaster=False, secondary=True, setName='rs', hosts=hosts)

        client = MongoClient(primary.uri, replicaSet='rs')
        self.addCleanup(client.close)
        wait_until(lambda: client.primary == primary.address,
                   'discover primary')

        topology = client._topology
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetWithPrimary,
                         topology.description.topology_type)

        # Open a socket in the application pool (calls ismaster).
        with going(client.db.command, 'buildinfo'):
            primary.receives('buildinfo').ok()

        # The primary hangs replying to ismaster.
        ismaster_future = Future()
        primary.autoresponds('ismaster',
                             lambda r: r.ok(ismaster_future.result()))

        # Network error on application operation.
        with self.assertRaises(ConnectionFailure):
            with going(client.db.command, 'buildinfo'):
                primary.receives('buildinfo').hangup()

        # Topology type is updated.
        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetNoPrimary,
                         topology.description.topology_type)

        # Let ismasters through again.
        ismaster_future.set_result(primary_response)

        # Demand a primary.
        with going(client.db.command, 'buildinfo'):
            wait_until(lambda: client.primary == primary.address,
                       'rediscover primary')
            primary.receives('buildinfo').ok()

        self.assertEqual(TOPOLOGY_TYPE.ReplicaSetWithPrimary,
                         topology.description.topology_type)


if __name__ == '__main__':
    unittest.main()
