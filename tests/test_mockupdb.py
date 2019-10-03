#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Test MockupDB."""

import contextlib
import datetime
import os
import ssl
import sys
import tempfile
import unittest
from struct import Struct

if sys.version_info[0] < 3:
    from io import BytesIO as StringIO
else:
    from io import StringIO

try:
    from queue import Queue
except ImportError:
    from Queue import Queue

from bson import (Binary, BSON, Code, DBRef, Decimal128, MaxKey, MinKey,
                  ObjectId, Regex, SON, Timestamp)
from bson.codec_options import CodecOptions
from pymongo import MongoClient, message, WriteConcern

from mockupdb import (go, going, Command, CommandBase, Matcher, MockupDB,
                      Request, OpInsert, OP_MSG_FLAGS, OpMsg, OpQuery,
                      QUERY_FLAGS)


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


class TestUnacknowledgedWrites(unittest.TestCase):
    def setUp(self):
        self.server = MockupDB(auto_ismaster=True)
        self.server.run()
        self.addCleanup(self.server.stop)
        self.client = MongoClient(self.server.uri)
        self.collection = self.client.db.get_collection(
            'collection', write_concern=WriteConcern(w=0))

    def test_insert_one(self):
        with going(self.collection.insert_one, {'_id': 1}):
            # The moreToCome flag = 2.
            self.server.receives(
                OpMsg('insert', 'collection', writeConcern={'w': 0}, flags=2))

    def test_insert_many(self):
        collection = self.collection.with_options(
            write_concern=WriteConcern(0))

        docs = [{'_id': 1}, {'_id': 2}]
        with going(collection.insert_many, docs, ordered=False):
            self.server.receives(OpMsg(SON([
                ('insert', 'collection'),
                ('ordered', False),
                ('writeConcern', {'w': 0})]), flags=2))

    def test_replace_one(self):
        with going(self.collection.replace_one, {}, {}):
            self.server.receives(OpMsg(SON([
                ('update', 'collection'),
                ('writeConcern', {'w': 0})
            ]), flags=2))

    def test_update_many(self):
        with going(self.collection.update_many, {}, {'$unset': 'a'}):
            self.server.receives(OpMsg(SON([
                ('update', 'collection'),
                ('ordered', True),
                ('writeConcern', {'w': 0})
            ]), flags=2))

    def test_delete_one(self):
        with going(self.collection.delete_one, {}):
            self.server.receives(OpMsg(SON([
                ('delete', 'collection'),
                ('writeConcern', {'w': 0})
            ]), flags=2))

    def test_delete_many(self):
        with going(self.collection.delete_many, {}):
            self.server.receives(OpMsg(SON([
                ('delete', 'collection'),
                ('writeConcern', {'w': 0})]), flags=2))


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

    def test_bson_classes(self):
        _id = '5a918f9fa08bff9c7688d3e1'

        for a, b in [
            (Binary(b'foo'), Binary(b'foo')),
            (Code('foo'), Code('foo')),
            (Code('foo', {'x': 1}), Code('foo', {'x': 1})),
            (DBRef('coll', 1), DBRef('coll', 1)),
            (DBRef('coll', 1, 'db'), DBRef('coll', 1, 'db')),
            (Decimal128('1'), Decimal128('1')),
            (MaxKey(), MaxKey()),
            (MinKey(), MinKey()),
            (ObjectId(_id), ObjectId(_id)),
            (Regex('foo', 'i'), Regex('foo', 'i')),
            (Timestamp(1, 2), Timestamp(1, 2)),
        ]:
            # Basic case.
            self.assertTrue(
                Matcher(Command(y=b)).matches(Command(y=b)),
                "MockupDB %r doesn't equal itself" % (b,))

            # First Command argument is special, try comparing the second also.
            self.assertTrue(
                Matcher(Command('x', y=b)).matches(Command('x', y=b)),
                "MockupDB %r doesn't equal itself" % (b,))

            # In practice, users pass PyMongo classes in message specs.
            self.assertTrue(
                Matcher(Command(y=b)).matches(Command(y=a)),
                "PyMongo %r != MockupDB %r" % (a, b))

            self.assertTrue(
                Matcher(Command('x', y=b)).matches(Command('x', y=a)),
                "PyMongo %r != MockupDB %r" % (a, b))

    def test_datetime(self):
        server = MockupDB(auto_ismaster=True)
        server.run()
        client = MongoClient(server.uri)
        # Python datetimes have microsecond precision, BSON only millisecond.
        # Ensure this datetime matches itself despite the truncation.
        dt = datetime.datetime(2018, 12, 1, 6, 6, 6, 12345)
        doc = SON([('_id', 1), ('dt', dt)])
        with going(client.db.collection.insert_one, doc):
            server.receives(
                OpMsg('insert', 'collection', documents=[doc])).ok()


class TestAutoresponds(unittest.TestCase):
    def test_auto_dequeue(self):
        server = MockupDB(auto_ismaster=True)
        server.run()
        client = MongoClient(server.uri)
        future = go(client.admin.command, 'ping')
        server.autoresponds('ping')  # Should dequeue the request.
        future()

    def test_autoresponds_case_insensitive(self):
        server = MockupDB(auto_ismaster=True)
        # Little M. Note this is only case-insensitive because it's a Command.
        server.autoresponds(CommandBase('fooBar'), foo='bar')
        server.run()
        response = MongoClient(server.uri).admin.command('Foobar')
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

    def test_default_wire_version(self):
        server = MockupDB(auto_ismaster=True)
        server.run()
        self.addCleanup(server.stop)
        ismaster = MongoClient(server.uri).admin.command('isMaster')
        self.assertEqual(ismaster['minWireVersion'], 0)
        self.assertEqual(ismaster['maxWireVersion'], 6)

    def test_wire_version(self):
        server = MockupDB(auto_ismaster=True,
                          min_wire_version=1,
                          max_wire_version=42)
        server.run()
        self.addCleanup(server.stop)
        ismaster = MongoClient(server.uri).admin.command('isMaster')
        self.assertEqual(ismaster['minWireVersion'], 1)
        self.assertEqual(ismaster['maxWireVersion'], 42)

    @unittest.skipIf(sys.platform == 'win32', 'Windows')
    def test_unix_domain_socket(self):
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.sock')
        tmp.close()
        server = MockupDB(auto_ismaster={'maxWireVersion': 3},
                          uds_path=tmp.name)
        server.run()
        self.assertTrue(server.uri.endswith('.sock'),
                        'Expected URI "%s" to end with ".sock"' % (server.uri,))
        self.assertEqual(server.host, tmp.name)
        self.assertEqual(server.port, 0)
        self.assertEqual(server.address, (tmp.name, 0))
        self.assertEqual(server.address_string, tmp.name)
        client = MongoClient(server.uri)
        with going(client.test.command, {'foo': 1}) as future:
            server.receives().ok()

        response = future()
        self.assertEqual(1, response['ok'])
        server.stop()
        self.assertFalse(os.path.exists(tmp.name))


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


class TestOpMsg(unittest.TestCase):
    def setUp(self):
        self.server = MockupDB(auto_ismaster={'maxWireVersion': 6})
        self.server.run()
        self.addCleanup(self.server.stop)
        self.client = MongoClient(self.server.uri)

    def test_flags(self):
        doc = SON([('foo', 1), ('$db', 'mydb')])
        obj = BSON.encode(doc)

        for flag_name, flag_bit in OP_MSG_FLAGS.items():
            # MockupDB strips 16-byte header then calls unpack on body.
            message_body = b''.join([
                Struct('<I').pack(flag_bit),  # flagBits
                Struct('<b').pack(0),  # section kind
                obj,
            ])

            if flag_name == 'checksumPresent':
                message_body += Struct('<I').pack(1234)

            op_msg = OpMsg.unpack(msg=message_body,
                                  client=None,
                                  server=None,
                                  request_id=0)

            self.assertEqual(op_msg.flags, flag_bit)
            self.assertEqual(op_msg.doc, doc)
            self.assertEqual(op_msg.namespace, 'mydb')

            if flag_name == 'checksumPresent':
                self.assertEqual(op_msg.checksum, 1234)
            else:
                self.assertEqual(op_msg.checksum, None)


if __name__ == '__main__':
    unittest.main()
