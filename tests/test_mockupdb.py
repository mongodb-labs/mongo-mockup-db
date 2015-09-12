#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Test MockupDB."""

import contextlib
import ssl
import sys

if sys.version_info[0] < 3:
    from io import BytesIO as StringIO
else:
    from io import StringIO

try:
    from queue import Queue
except ImportError:
    from Queue import Queue

# Tests depend on PyMongo's BSON implementation, but MockupDB itself does not.
from bson import SON
from bson.codec_options import CodecOptions
from pymongo import MongoClient, message, WriteConcern

from mockupdb import (go, going,
                      Command, Matcher, MockupDB, Request,
                      OpDelete, OpInsert, OpQuery, OpUpdate,
                      DELETE_FLAGS, INSERT_FLAGS, UPDATE_FLAGS, QUERY_FLAGS)

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


class TestRequest(unittest.TestCase):
    def _pack_request(self, ns, slave_ok):
        flags = 4 if slave_ok else 0
        request_id, msg_bytes, max_doc_size = message.query(
            flags, ns, 0, 0, {}, None, CodecOptions())

        # Skip 16-byte standard header.
        return msg_bytes[16:], request_id

    def test_flags(self):
        request = Request()
        self.assertIsNone(request.flags)
        self.assertFalse(request.slave_ok)

        msg_bytes, request_id = self._pack_request('db.collection', False)
        request = OpQuery.unpack(msg_bytes, None, None, request_id)
        self.assertIsInstance(request, OpQuery)
        self.assertNotIsInstance(request, Command)
        self.assertEqual(0, request.flags)
        self.assertFalse(request.slave_ok)
        self.assertFalse(request.slave_okay)  # Synonymous.

        msg_bytes, request_id = self._pack_request('db.$cmd', False)
        request = OpQuery.unpack(msg_bytes, None, None, request_id)
        self.assertIsInstance(request, Command)
        self.assertEqual(0, request.flags)

        msg_bytes, request_id = self._pack_request('db.collection', True)
        request = OpQuery.unpack(msg_bytes, None, None, request_id)
        self.assertEqual(4, request.flags)
        self.assertTrue(request.slave_ok)

        msg_bytes, request_id = self._pack_request('db.$cmd', True)
        request = OpQuery.unpack(msg_bytes, None, None, request_id)
        self.assertEqual(4, request.flags)

    def test_fields(self):
        self.assertIsNone(OpQuery({}).fields)
        self.assertEqual({'_id': False, 'a': 1},
                         OpQuery({}, fields={'_id': False, 'a': 1}).fields)

    def test_repr(self):
        self.assertEqual('Request()', repr(Request()))
        self.assertEqual('Request({})', repr(Request({})))
        self.assertEqual('Request({})', repr(Request([{}])))
        self.assertEqual('Request(flags=4)', repr(Request(flags=4)))

        self.assertEqual('OpQuery({})', repr(OpQuery()))
        self.assertEqual('OpQuery({})', repr(OpQuery({})))
        self.assertEqual('OpQuery({})', repr(OpQuery([{}])))
        self.assertEqual('OpQuery({}, flags=SlaveOkay)',
                         repr(OpQuery(flags=4)))
        self.assertEqual('OpQuery({}, flags=SlaveOkay)',
                         repr(OpQuery({}, flags=4)))
        self.assertEqual('OpQuery({}, flags=TailableCursor|AwaitData)',
                         repr(OpQuery({}, flags=34)))

        self.assertEqual('Command({})', repr(Command()))
        self.assertEqual('Command({"foo": 1})', repr(Command('foo')))
        son = SON([('b', 1), ('a', 1), ('c', 1)])
        self.assertEqual('Command({"b": 1, "a": 1, "c": 1})',
                         repr(Command(son)))
        self.assertEqual('Command({}, flags=SlaveOkay)',
                         repr(Command(flags=4)))

        self.assertEqual('OpInsert({}, {})', repr(OpInsert([{}, {}])))
        self.assertEqual('OpInsert({}, {})', repr(OpInsert({}, {})))

    def test_assert_matches(self):
        request = OpQuery({'x': 17}, flags=QUERY_FLAGS['SlaveOkay'])
        request.assert_matches(request)

        with self.assertRaises(AssertionError):
            request.assert_matches(Command('foo'))


class TestLegacyWrites(unittest.TestCase):
    def setUp(self):
        self.server = MockupDB(auto_ismaster=True)
        self.server.run()
        self.addCleanup(self.server.stop)
        self.client = MongoClient(self.server.uri)
        self.collection = self.client.db.collection

    def test_insert_one(self):
        with going(self.collection.insert_one, {'_id': 1}) as future:
            self.server.receives(OpInsert({'_id': 1}, flags=0))
            self.server.receives(Command('getlasterror')).replies_to_gle()

        self.assertEqual(1, future().inserted_id)

    def test_insert_many(self):
        collection = self.collection.with_options(
            write_concern=WriteConcern(0))

        flags = INSERT_FLAGS['ContinueOnError']
        docs = [{'_id': 1}, {'_id': 2}]
        with going(collection.insert_many, docs, ordered=False) as future:
            self.server.receives(OpInsert(docs, flags=flags))

        self.assertEqual([1, 2], future().inserted_ids)

    def test_replace_one(self):
        with going(self.collection.replace_one, {}, {}) as future:
            self.server.receives(OpUpdate({}, {}, flags=0))
            request = self.server.receives(Command('getlasterror'))
            request.replies_to_gle(upserted=1)

        self.assertEqual(1, future().upserted_id)

    def test_update_many(self):
        flags = UPDATE_FLAGS['MultiUpdate']
        with going(self.collection.update_many, {}, {'$unset': 'a'}) as future:
            update = self.server.receives(OpUpdate({}, {}, flags=flags))
            self.assertEqual(2, update.flags)
            gle = self.server.receives(Command('getlasterror'))
            gle.replies_to_gle(upserted=1)

        self.assertEqual(1, future().upserted_id)

    def test_delete_one(self):
        flags = DELETE_FLAGS['SingleRemove']
        with going(self.collection.delete_one, {}) as future:
            delete = self.server.receives(OpDelete({}, flags=flags))
            self.assertEqual(1, delete.flags)
            gle = self.server.receives(Command('getlasterror'))
            gle.replies_to_gle(n=1)

        self.assertEqual(1, future().deleted_count)

    def test_delete_many(self):
        with going(self.collection.delete_many, {}) as future:
            delete = self.server.receives(OpDelete({}, flags=0))
            self.assertEqual(0, delete.flags)
            gle = self.server.receives(Command('getlasterror'))
            gle.replies_to_gle(n=2)

        self.assertEqual(2, future().deleted_count)


class TestMatcher(unittest.TestCase):
    def test_command_name_case_insensitive(self):
        self.assertTrue(
            Matcher(Command('ismaster')).matches(Command('IsMaster')))

    def test_command_first_arg(self):
        self.assertFalse(
            Matcher(Command(ismaster=1)).matches(Command(ismaster=2)))

    def test_command_fields(self):
        self.assertTrue(
            Matcher(Command('a', b=1)).matches(Command('a', b=1)))

        self.assertFalse(
            Matcher(Command('a', b=1)).matches(Command('a', b=2)))


class TestAutoresponds(unittest.TestCase):
    def test_auto_dequeue(self):
        server = MockupDB(auto_ismaster=True)
        server.run()
        client = MongoClient(server.uri)
        future = go(client.admin.command, 'ping')
        server.autoresponds('ping')  # Should dequeue the request.
        future()

    def test_autoresponds_case_insensitive(self):
        server = MockupDB()
        # Little M. Note this is only case-insensitive because it's a Command.
        server.autoresponds(Command('ismaster'), foo='bar')
        server.run()
        response = MongoClient(server.uri).admin.command('isMaster')  # Big M.
        self.assertEqual('bar', response['foo'])


class TestSSL(unittest.TestCase):
    def test_ssl_uri(self):
        server = MockupDB(ssl=True)
        server.run()
        self.addCleanup(server.stop)
        self.assertEqual(
            'mongodb://localhost:%d/?ssl=true' % server.port,
            server.uri)

    def test_ssl_basic(self):
        server = MockupDB(ssl=True, auto_ismaster=True)
        server.run()
        self.addCleanup(server.stop)
        client = MongoClient(server.uri, ssl_cert_reqs=ssl.CERT_NONE)
        client.db.command('ismaster')


class TestMockupDB(unittest.TestCase):
    def test_iteration(self):
        server = MockupDB(auto_ismaster={'maxWireVersion': 3})
        server.run()
        self.addCleanup(server.stop)
        client = MongoClient(server.uri)

        def send_three_docs():
            for i in range(3):
                client.test.test.insert({'_id': i})

        with going(send_three_docs):
            j = 0

            # The "for request in server" statement is the point of this test.
            for request in server:
                self.assertTrue(request.matches({'insert': 'test',
                                                 'documents': [{'_id': j}]}))

                request.ok()
                j += 1
                if j == 3:
                    break


class TestResponse(unittest.TestCase):
    def test_ok(self):
        server = MockupDB(auto_ismaster={'maxWireVersion': 3})
        server.run()
        self.addCleanup(server.stop)
        client = MongoClient(server.uri)

        with going(client.test.command, {'foo': 1}) as future:
            server.receives().ok(3)

        response = future()
        self.assertEqual(3, response['ok'])


if __name__ == '__main__':
    unittest.main()
