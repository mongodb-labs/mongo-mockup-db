#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Test MockupDB."""

import time

from tests import unittest  # unittest2 on Python 2.6.


try:
    from queue import Queue, Empty
except ImportError:
    from Queue import Queue, Empty

from pymongo import MongoClient, ReadPreference

from mockupdb import MockupDB, go, OpReply


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


if __name__ == '__main__':
    unittest.main()
