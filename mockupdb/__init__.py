#  -*- coding: utf-8 -*-
# Copyright 2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Simulate a MongoDB server."""

__author__ = 'A. Jesse Jiryu Davis'
__email__ = 'jesse@mongodb.com'
__version__ = '0.1.0'

import collections
import contextlib
import errno
import functools
import inspect
import os
import random
import select
import socket
import struct
import threading
import time
import weakref
import sys
from codecs import utf_8_decode as _utf_8_decode

try:
    from queue import Queue, Empty
except ImportError:
    from Queue import Queue, Empty

try:
    from collections import OrderedDict
except:
    from ordereddict import OrderedDict  # Python 2.6, "pip install ordereddict"

import bson  # From pymongo package.

PY3 = sys.version_info[0] == 3
if PY3:
    string_type = str

    def reraise(exctype, value, trace=None):
        raise exctype(str(value)).with_traceback(trace)
else:
    string_type = basestring

    # "raise x, y, z" raises SyntaxError in Python 3.
    exec("""def reraise(exctype, value, trace=None):
    raise exctype, str(value), trace
""")


# Do not export "main".
__all__ = [
    'MockupDB', 'go', 'interactive_server',

    'OP_REPLY', 'OP_UPDATE', 'OP_INSERT', 'OP_QUERY', 'OP_GET_MORE',
    'OP_DELETE', 'OP_KILL_CURSORS',

    'QUERY_FLAGS', 'UPDATE_FLAGS', 'INSERT_FLAGS', 'DELETE_FLAGS',
    'REPLY_FLAGS',

    'Request', 'Command', 'OpQuery', 'OpGetMore', 'OpKillCursors', 'OpInsert',
    'OpDelete', 'OpReply',

    'Matcher',
]


def go(fn, *args, **kwargs):
    """TODO: doc."""
    result = [None]
    error = []
    event = threading.Event()

    def target():
        try:
            result[0] = fn(*args, **kwargs)
        except Exception:
            error.extend(sys.exc_info())
        event.set()

    t = threading.Thread(target=target)
    t.daemon = True
    t.start()

    def get_result(timeout=10):
        event.wait(timeout)
        if not event.is_set():
            raise AssertionError('timed out waiting for %r' % fn)
        if error:
            reraise(*error)
        return result[0]

    return get_result


OP_REPLY = 1
OP_UPDATE = 2001
OP_INSERT = 2002
OP_QUERY = 2004
OP_GET_MORE = 2005
OP_DELETE = 2006
OP_KILL_CURSORS = 2007

QUERY_FLAGS = OrderedDict([
    ('TailableCursor', 2),
    ('SlaveOkay', 4),
    ('OplogReplay', 8),
    ('NoTimeout', 16),
    ('AwaitData', 32),
    ('Exhaust', 64),
    ('Partial', 128)])

UPDATE_FLAGS = OrderedDict([
    ('Upsert', 1),
    ('MultiUpdate', 2)])

INSERT_FLAGS = OrderedDict([
    ('ContinueOnError', 1)])

DELETE_FLAGS = OrderedDict([
    ('SingleRemove', 1)])

REPLY_FLAGS = OrderedDict([
    ('CursorNotFound', 1),
    ('QueryFailure', 2)])

_UNPACK_INT = struct.Struct("<i").unpack
_UNPACK_LONG = struct.Struct("<q").unpack


def _get_c_string(data, position):
    """Decode a BSON 'C' string to python unicode string."""
    end = data.index(b"\x00", position)
    return _utf_8_decode(data[position:end], None, True)[0], end + 1


class _PeekableQueue(Queue):
    """Only safe from one consumer thread at a time."""
    _NO_ITEM = object()

    def __init__(self, *args, **kwargs):
        Queue.__init__(self, *args, **kwargs)
        self._item = _PeekableQueue._NO_ITEM

    def peek(self, block=True, timeout=None):
        if self._item is not _PeekableQueue._NO_ITEM:
            return self._item
        else:
            self._item = self.get(block, timeout)
            return self._item

    def get(self, block=True, timeout=None):
        if self._item is not _PeekableQueue._NO_ITEM:
            item = self._item
            self._item = _PeekableQueue._NO_ITEM
            return item
        else:
            return Queue.get(self, block, timeout)


class Request(object):
    """Base class for `Command`, `OpInsert`, and so on."""
    opcode = None

    def __init__(self, *args, **kwargs):
        self._flags = kwargs.pop('flags', None)
        self._namespace = kwargs.pop('namespace', None)
        self._client = kwargs.pop('client', None)
        self._request_id = kwargs.pop('request_id', None)
        self._server = kwargs.pop('server', None)
        self._docs = make_docs(*args, **kwargs)

    # TODO: remove.
    def matches(self, *args, **kwargs):
        request = make_request(*args, **kwargs)
        if self.opcode != request.opcode:
            return False
        if self._namespace not in (None, request.namespace):
            return False
        if self._flags not in (None, request._flags):
            return False
        if len(self._docs) != len(request.docs):
            return False
        for i, doc in enumerate(self._docs):
            for key, value in doc.items():
                if request.docs[i].get(key) != value:
                    return False
        return True

    @property
    def doc(self):
        """The request document, if there is exactly one.

        Use this for queries, commands, and legacy deletes. Legacy writes may
        have many documents, OP_GET_MORE and OP_KILL_CURSORS have none.
        """
        assert len(self.docs) == 1, '%r has more than one document' % self
        return self.docs[0]

    @property
    def docs(self):
        """The request documents, if any."""
        return self._docs

    @property
    def namespace(self):
        """The operation namespace or None."""
        return self._namespace

    @property
    def flags(self):
        """The request flags or None."""
        return self._flags

    @property
    def request_id(self):
        """The request id or None."""
        return self._request_id

    @property
    def client_port(self):
        """Client connection's TCP port."""
        return self._client.getpeername()[1]

    def replies(self, *args, **kwargs):
        """Send an `OpReply` to the client.

        The default reply to a command is ``{'ok': 1}``, otherwise the default
        is empty (no documents).
        """
        self._replies(*args, **kwargs)

    ok = send = sends = reply = replies
    """Synonym for `replies`."""

    def fail(self, err='MockupDB query failure', *args, **kwargs):
        """Reply to a query with the QueryFailure flag and an '$err' key."""
        kwargs.setdefault('flags', 0)
        kwargs['flags'] |= REPLY_FLAGS['QueryFailure']
        kwargs['$err'] = err
        self.replies(*args, **kwargs)

    def command_err(self, code=1, errmsg='MockupDB command failure',
                    *args, **kwargs):
        """Error reply to a command."""
        kwargs.setdefault('ok', 0)
        kwargs['code'] = code
        kwargs['errmsg'] = errmsg
        self.replies(*args, **kwargs)

    def hangup(self):
        """Close the connection."""
        self._client.close()

    def _replies(self, *args, **kwargs):
        """Overridable method."""
        reply_msg = make_reply(*args, **kwargs)
        if self._server and self._server.verbose:
            print('\t%d\t<-- %r' % (self.client_port, reply_msg))
        reply_bytes = reply_msg.reply_bytes(self)
        self._client.sendall(reply_bytes)

    def __str__(self):
        if len(self.docs) > 1:
            return str(self.docs)
        else:
            return str(self.docs[0])

    def __repr__(self):
        name = self.__class__.__name__
        if not self.docs:
            rep = '%s(' % name
        elif len(self.docs) == 1:
            rep = '%s(%s' % (name, self.doc)
        else:
            rep = '%s(%s' % (name, ', '.join(str(doc) for doc in self._docs))

        if self._flags:
            rep += ', flags=%s' % (
                '|'.join(name for name, value in QUERY_FLAGS.items()
                         if self._flags & value))

        return rep + ')'


# TODO: remove
class AnyRequest(object):
    """Matches any client request.

    To always fail::

        >>> server = MockupDB()
        >>> server.autoresponds(Request(), ok=0)
    """
    def matches(self, _):
        return True

    def __repr__(self):
        return self.__class__.__name__


class OpQuery(Request):
    """A query (besides a command) the client executes on the server."""
    opcode = OP_QUERY

    @classmethod
    def unpack(cls, msg, client, server, request_id):
        """Parse message and return an `OpQuery` or `Command`.

        Takes the client message as bytes, the client and server socket objects,
        and the client request id.
        """
        flags, = _UNPACK_INT(msg[:4])
        namespace, pos = _get_c_string(msg, 4)
        is_command = namespace.endswith('.$cmd')
        num_to_skip, = _UNPACK_INT(msg[pos:pos + 4])
        pos += 4
        num_to_return, = _UNPACK_INT(msg[pos:pos + 4])
        pos += 4
        docs = bson.decode_all(msg[pos:])
        if is_command:
            assert len(docs) == 1
            command_ns = namespace[:len('.$cmd')]
            return Command(docs, namespace=command_ns, client=client,
                           request_id=request_id, server=server)
        else:
            if len(docs) == 1:
                fields = None
            else:
                assert len(docs) == 2
                fields = docs[1]
            return OpQuery(docs[0], fields=fields, namespace=namespace,
                           flags=flags, num_to_skip=num_to_skip,
                           num_to_return=num_to_return, client=client,
                           request_id=request_id, server=server)

    def __init__(self, *args, **kwargs):
        fields = kwargs.pop('fields', None)
        if fields is not None and not isinstance(fields, collections.Mapping):
            raise TypeError('fields must be a dict')
        self._fields = fields
        self._num_to_skip = kwargs.pop('num_to_skip', None)
        self._num_to_return = kwargs.pop('num_to_return', None)
        super(OpQuery, self).__init__(*args, **kwargs)
        if not self._docs:
            self._docs = [{}]  # Default query filter.
        elif len(self._docs) > 1:
            raise ValueError('OpQuery too many documents: %s'
                             % self._docs)

    def matches(self, message):
        """Match an `OpQuery`

        The message's flags, num_to_skip, and num_to_return must match, and
        its filter document must be a subset of this one.
        """
        if isinstance(message, Command):
            return False
        if not super(OpQuery, self).matches(message):
            return False
        if self._num_to_skip not in (None, message.num_to_skip):
            return False
        return self._num_to_return in (None, message.num_to_return)

    @property
    def num_to_skip(self):
        """Client query's numToSkip or None."""
        return self._num_to_skip

    @property
    def num_to_return(self):
        """Client query's numToReturn or None."""
        return self._num_to_return

    @property
    def fields(self):
        """Client query's fields selector or None."""
        # TODO: test
        return self._fields

    def __repr__(self):
        rep = super(OpQuery, self).__repr__().rstrip(')')
        if self._num_to_skip is not None:
            rep += ', numToSkip=%d' % self._num_to_skip
        if self._num_to_return is not None:
            rep += ', numToReturn=%d' % self._num_to_return
        return rep + ')'


class Command(OpQuery):
    """A command the client executes on the server."""

    def _replies(self, *args, **kwargs):
        reply = make_reply(*args, **kwargs)
        if not reply.docs:
            reply.docs = [{'ok': 1}]
        else:
            if len(reply.docs) > 1:
                raise ValueError('Command reply with multiple documents: %s'
                                 % reply.docs)
            reply.doc.setdefault('ok', 1)
        super(Command, self)._replies(reply)

    def replies_to_gle(self, **kwargs):
        """Send a getlasterror response.

        Defaults to ``{ok: 1, err: null}``. Add or override values by passing
        keyword arguments.
        """
        kwargs.setdefault('err', None)
        self.replies(**kwargs)


class OpGetMore(Request):
    """An OP_GET_MORE the client executes on the server."""
    @classmethod
    def unpack(cls, msg, client, server, request_id):
        """Parse message and return an `OpGetMore`.

        Takes the client message as bytes, the client and server socket objects,
        and the client request id.
        """
        flags, = _UNPACK_INT(msg[:4])
        namespace, pos = _get_c_string(msg, 4)
        num_to_return, = _UNPACK_INT(msg[pos:pos + 4])
        pos += 4
        cursor_id = _UNPACK_LONG(msg[pos:pos + 8])
        return OpGetMore(namespace=namespace, flags=flags, client=client,
                         num_to_return=num_to_return, cursor_id=cursor_id,
                         request_id=request_id, server=server)

    def __init__(self, **kwargs):
        self._num_to_return = kwargs.pop('num_to_return', None)
        self._cursor_id = kwargs.pop('cursor_id', None)
        super(OpGetMore, self).__init__(**kwargs)

    @property
    def num_to_return(self):
        """The client message's numToReturn field."""
        return self._num_to_return


class OpKillCursors(Request):
    """An OP_KILL_CURSORS the client executes on the server."""
    @classmethod
    def unpack(cls, msg, client, server, _):
        """Parse message and return an `OpKillCursors`.

        Takes the client message as bytes, the client and server socket objects,
        and the client request id.
        """
        # Leading 4 bytes are reserved.
        num_of_cursor_ids, = _UNPACK_INT(msg[4:8])
        cursor_ids = []
        pos = 8
        for _ in range(num_of_cursor_ids):
            cursor_ids.append(_UNPACK_INT(msg[pos:pos+4])[0])
            pos += 4
        return OpKillCursors(client=client, cursor_ids=cursor_ids,
                             server=server)

    def __init__(self, **kwargs):
        self._cursor_ids = kwargs.pop('cursor_ids', None)
        super(OpKillCursors, self).__init__(**kwargs)

    def matches(self, message):
        if not super(OpKillCursors, self).matches(message):
            return False
        return self._cursor_ids in (None, message.cursor_ids)

    @property
    def cursor_ids(self):
        """List of cursor ids the client wants to kill."""
        return self._cursor_ids

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self._cursor_ids)


class _LegacyWrite(Request):
    @classmethod
    def unpack(cls, msg, client, server, request_id):
        """Parse message and return an `OpInsert`.

        Takes the client message as bytes, the client and server socket objects,
        and the client request id.
        """
        flags, = _UNPACK_INT(msg[:4])
        namespace, pos = _get_c_string(msg, 4)
        docs = bson.decode_all(msg[pos:])
        return cls(*docs, namespace=namespace, flags=flags, client=client,
                   request_id=request_id, server=server)


class OpInsert(_LegacyWrite):
    """A legacy OP_INSERT the client executes on the server."""
    opcode = OP_INSERT


class OpUpdate(_LegacyWrite):
    """A legacy OP_UPDATE the client executes on the server."""
    opcode = OP_UPDATE


class OpDelete(_LegacyWrite):
    """A legacy OP_DELETE the client executes on the server."""
    opcode = OP_DELETE


class OpReply(object):
    """A reply from `MockupDB` to the MongoClient."""
    def __init__(self, *args, **kwargs):
        self._flags = kwargs.pop('flags', 0)
        self._cursor_id = kwargs.pop('cursor_id', 0)
        self._starting_from = kwargs.pop('starting_from', 0)
        self._docs = make_docs(*args, **kwargs)

    @property
    def docs(self):
        """The reply documents, if any."""
        return self._docs

    @docs.setter
    def docs(self, docs):
        self._docs = make_docs(docs)

    @property
    def doc(self):
        """Contents of reply.

        Useful for replies to commands; replies to other messages may have no
        documents or multiple documents.
        """
        assert len(self._docs) == 1, '%s has more than one document' % self
        return self._docs[0]

    def update(self, *args, **kwargs):
        """Update the document. Same as ``dict().update()``.

           >>> reply = OpReply({'ismaster': True})
           >>> reply.update(maxWireVersion=3)
           >>> reply.doc['maxWireVersion']
           3
           >>> reply.update({'maxWriteBatchSize': 10, 'msg': 'isdbgrid'})
        """
        self.doc.update(*args, **kwargs)

    def reply_bytes(self, request):
        """Take a `Request` and return an OP_REPLY message as bytes."""
        flags = struct.pack("<i", self._flags)
        cursor_id = struct.pack("<q", self._cursor_id)
        starting_from = struct.pack("<i", self._starting_from)
        number_returned = struct.pack("<i", len(self._docs))
        reply_id = random.randint(0, 1000000)
        response_to = request.request_id

        data = b''.join([flags, cursor_id, starting_from, number_returned])
        data += b''.join([bson.BSON.encode(doc) for doc in self._docs])

        message = struct.pack("<i", 16 + len(data))
        message += struct.pack("<i", reply_id)
        message += struct.pack("<i", response_to)
        message += struct.pack("<i", OP_REPLY)
        return message + data

    def __str__(self):
        if len(self._docs) == 1:
            return str(self._docs[0])
        else:
            return str(self._docs)

    def __repr__(self):
        rep = '%s(%s' % (self.__class__.__name__, self)
        if self._starting_from:
            rep += ', starting_from=%d' % self._starting_from
        return rep + ')'


class Matcher(object):
    """Matches a subset of `.Request` objects.

    Initialized with a `request spec`_.

    Used by `~MockupDB.receives` to assert the client sent the expected request,
    and by `~MockupDB.got` to test if it did and return ``True`` or ``False``.
    Used by `.autoresponds` to match requests with autoresponses.
    """
    opcode = None  # Default.

    def __init__(self, *args, **kwargs):
        self._kwargs = kwargs
        self._prototype = make_prototype_request(*args, **kwargs)
        if args or kwargs:
            self.opcode = self._prototype.opcode

    def matches(self, *args, **kwargs):
        """Take a `request spec`_ and return ``True`` or ``False``.

        .. request-matching rules::

        The empty matcher matches anything:

        >>> Matcher().matches({'a': 1})
        True
        >>> Matcher().matches({'a': 1}, {'a': 1})
        True
        >>> Matcher().matches('ismaster')
        True

        A matcher's document matches if its key-value pairs are a subset of the
        request's:

        >>> Matcher({'a': 1}).matches({'a': 1})
        True
        >>> Matcher({'a': 2}).matches({'a': 1})
        False
        >>> Matcher({'a': 1}).matches({'a': 1, 'b': 1})
        True

        The matcher must have the same number of documents as the request:

        >>> Matcher().matches()
        True
        >>> Matcher([]).matches([])
        True
        >>> Matcher({'a': 2}).matches({'a': 1}, {'a': 1})
        False

        By default, it matches any opcode:

        >>> m = Matcher()
        >>> m.matches(OpQuery)
        True
        >>> m.matches(OpInsert)
        True

        You can specify what request opcode to match:

        >>> m = Matcher(OpQuery)
        >>> m.matches(OpInsert, {'_id': 1})
        False
        >>> m.matches(OpQuery, {'_id': 1})
        True

        Commands are queries, too:

        >>> m.matches(Command)
        True

        It matches properties specific to certain opcodes:

        >>> m = Matcher(OpGetMore, num_to_return=3)
        >>> m.matches(OpGetMore())
        False
        >>> m.matches(OpGetMore(num_to_return=2))
        False
        >>> m.matches(OpGetMore(num_to_return=3))
        True
        >>> m = Matcher(OpQuery(namespace='db.collection'))
        >>> m.matches(OpQuery)
        False
        >>> m.matches(OpQuery(namespace='db.collection'))
        True

        It matches any wire protocol header bits you specify:

        >>> m = Matcher(flags=QUERY_FLAGS['SlaveOkay'])
        >>> m.matches(OpQuery({'_id': 1}))
        False
        >>> m.matches(OpQuery({'_id': 1}, flags=QUERY_FLAGS['SlaveOkay']))
        True

        If you match on flags, be careful to also match on opcode. For example,
        if you simply check that the flag in bit position 0 is set:

        >>> m = Matcher(flags=INSERT_FLAGS['ContinueOnError'])

        ... you will match any request with that flag:

        >>> m.matches(OpDelete, flags=DELETE_FLAGS['SingleRemove'])
        True

        So specify the opcode, too:

        >>> m = Matcher(OpInsert, flags=INSERT_FLAGS['ContinueOnError'])
        >>> m.matches(OpDelete, flags=DELETE_FLAGS['SingleRemove'])
        False
        """
        # TODO: just take a Request, not args and kwargs?
        request = make_prototype_request(*args, **kwargs)
        if self.opcode not in (None, request.opcode):
            return False
        # for name, value in self._kwargs.items():
        #     prototype_value = getattr(self._prototype, name, None)
        #     actual_value = getattr(request, name, None)
        #     if prototype_value not in (None, actual_value):
        #         return False
        for name in dir(self._prototype):
            if name.startswith('_') or name in ('doc', 'docs'):
                # Ignore privates, and handle documents specially.
                continue
            prototype_value = getattr(self._prototype, name, None)
            if inspect.ismethod(prototype_value):
                continue
            actual_value = getattr(request, name, None)
            if prototype_value not in (None, actual_value):
                return False
        if len(self._prototype.docs) not in (0, len(request.docs)):
            return False
        for i, doc in enumerate(self._prototype.docs):
            for key, value in doc.items():
                if request.docs[i].get(key) != value:
                    return False
        return True

    @property
    def prototype(self):
        """The prototype `.Request` used to match actual requests with."""
        return self._prototype

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self._prototype)


def _synchronized(meth):
    """Call method while holding a lock."""
    @functools.wraps(meth)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return meth(self, *args, **kwargs)

    return wrapper


class MockupDB(object):
    """A simulated mongod or mongos.

    Call `run` to start the server, and always `close` it to avoid exceptions
    during interpreter shutdown.

    See the tutorial for comprehensive examples.

    :Optional parameters:
      - `port`: listening port number. If not specified, choose
        some unused port and return the port number from `run`.
      - `verbose`: if ``True``, print requests and replies to stdout.
      - `replicaSet`: a string, the replica set name. This only affects the
        value of `uri`.
      - `request_timeout`: seconds to wait for the next client request, or else
        assert. Default 10 seconds. Pass int(1e6) to disable.
      - `reply_timeout`: seconds to wait for a call to `Request.replies`, or
        else assert. Helps catch test bugs. Default 10 seconds. Pass int(1e6)
        to disable.
      - `auto_ismaster`: pass ``True`` to autorespond ``{'ok': 1}`` to
        ismaster requests, or pass a dict or `OpReply`.
    """
    def __init__(self, port=None, verbose=False, replicaSet=None,
                 request_timeout=10, reply_timeout=10, auto_ismaster=None):
        self._address = ('localhost', port)
        self._verbose = verbose
        self._replica_set = replicaSet  # TODO remove?

        # TODO: test & implement.
        self._request_timeout = request_timeout
        self._reply_timeout = reply_timeout

        self._listening_sock = None
        self._accept_thread = None
        self._server_socks = weakref.WeakSet()
        self._server_threads = weakref.WeakSet()
        self._stopped = False
        self._request_q = _PeekableQueue()
        self._requests_count = 0
        self._lock = threading.Lock()

        # List of (request_matcher, args, kwargs), where args and kwargs are
        # like those sent to request.reply().
        self._autoresponders = []
        if auto_ismaster is True:
            self.autoresponds('ismaster')
        elif auto_ismaster:
            self.autoresponds('ismaster', auto_ismaster)

    @_synchronized
    def run(self):
        """Begin serving. Returns the bound port."""
        self._listening_sock, self._address = bind_socket(self._address)
        self._accept_thread = threading.Thread(target=self._accept_loop)
        self._accept_thread.daemon = True
        self._accept_thread.start()
        return self.port

    @_synchronized
    def stop(self):
        """Stop serving. Always call this to clean up after yourself."""
        self._stopped = True
        threads = [self._accept_thread]
        threads.extend(self._server_threads)
        self._listening_sock.close()
        for sock in self._server_socks:
            sock.close()

        with self._unlock():
            for thread in threads:
                thread.join(10)

    def receives(self, *args, **kwargs):
        """Pop the next `Request` and assert it matches.

        Returns None if the server is stopped.

        Pass a `Request` or request pattern to specify what client request to
        expect. See the tutorial for examples. Pass ``timeout`` as a keyword
        argument to override this server's ``request_timeout``.
        """
        timeout = kwargs.pop('timeout', self._request_timeout)
        end = time.time() + timeout
        matcher = Matcher(*args, **kwargs)
        while not self._stopped:
            try:
                # Short timeout so we notice if the server is stopped.
                request = self._request_q.get(timeout=0.05)
            except Empty:
                if time.time() > end:
                    # TODO: show in doctest with timeout.
                    raise AssertionError('expected to receive %r, got nothing'
                                         % matcher.prototype)
            else:
                if matcher.matches(request):
                    return request
                else:
                    raise AssertionError('expected to receive %r, got %r'
                                         % (matcher.prototype, request))

    gets = pop = receive = receives
    """Synonym for `receives`."""

    @_synchronized
    def autoresponds(self, request, *args, **kwargs):
        """Send a canned reply to all matching client requests.
        
        ``request`` is a `Matcher` or an instance of `OpInsert`, `OpQuery`,
        etc. The remaining arguments are a `reply spec`_:

        >>> s = MockupDB()
        >>> s.autoresponds('ismaster')
        >>> s.autoresponds('foo')
        >>> s.autoresponds('bar', ok=0, errmsg='bad')
        >>> s.autoresponds('baz', {'key': 'value'})
        >>> s.autoresponds(OpQuery(namespace='db.collection'),
        ...                [{'_id': 1}, {'_id': 2}])
        >>> port = s.run()
        >>>
        >>> from pymongo import MongoClient
        >>> client = MongoClient(s.uri)
        >>> client.admin.command('ismaster')
        {u'ok': 1}
        >>> client.db.command('foo')
        {u'ok': 1}
        >>> client.db.command('bar')
        Traceback (most recent call last):
        ...
        OperationFailure: command SON([('bar', 1)]) on namespace db.$cmd failed: bad
        >>> client.db.command('baz')
        {u'ok': 1, u'key': u'value'}
        >>> list(client.db.collection.find())
        [{u'_id': 1}, {u'_id': 2}]

        If the request currently at the head of the queue matches, it is popped
        and replied to. Future matching requests skip the queue.

        Responders are applied in order, most recently added first, until one
        matches.
        """
        matcher = request if isinstance(request, Matcher) else Matcher(request)
        self._autoresponders.append((matcher, args, kwargs))
        try:
            request = self._request_q.peek(block=False)
        except Empty:
            return

        if matcher.matches(request):
            self._request_q.get_nowait().reply(*args, **kwargs)

    @property
    def address(self):
        """The listening (host, port)."""
        return self._address

    @property
    def address_string(self):
        """The listening "host:port"."""
        return '%s:%d' % self._address

    @property
    def host(self):
        """The listening hostname."""
        return self._address[0]

    @property
    def port(self):
        """The listening port."""
        return self._address[1]

    @property
    def uri(self):
        """Connection string to pass to `~pymongo.mongo_client.MongoClient`."""
        assert self.host and self.port
        uri = 'mongodb://%s:%s' % self._address
        if self._replica_set is not None:
            uri += '/?replicaSet=%s' % self._replica_set
        return uri

    @property
    def replica_set_name(self):
        """Replica set name or None.

        This is the value passed as ``replicaSet``. It only affects the `uri`.
        """
        return self._replica_set

    @property
    def verbose(self):
        """If verbose logging is turned on."""
        return self._verbose

    @property
    def requests_count(self):
        """Number of requests this server has received.

        Includes autoresponded requests.
        """
        return self._requests_count

    @property
    @_synchronized
    def running(self):
        """If this server is started and not stopped."""
        return self._accept_thread and not self._stopped

    def _accept_loop(self):
        """Accept client connections and spawn a thread for each."""
        self._listening_sock.setblocking(0)
        while not self._stopped:
            try:
                # Wait a short time to accept.
                if select.select([self._listening_sock.fileno()], [], [], 0.05):
                    client, client_addr = self._listening_sock.accept()
                    if self._verbose:
                        print('connection from %s:%s' % client_addr)
                    server_thread = threading.Thread(
                        target=functools.partial(self._server_loop, client))
                    server_thread.daemon = True
                    server_thread.start()
                    self._server_threads.add(server_thread)
            except socket.error as error:
                if error.errno not in (errno.EAGAIN, errno.EBADF):
                    raise
            except select.error as error:
                if error.args[0] == errno.EBADF:
                    # Closed.
                    break
                else:
                    raise

    @_synchronized
    def _server_loop(self, client):
        """Read requests. 'client' is a client socket."""
        while not self._stopped:
            try:
                with self._unlock():
                    request_msg = mock_server_receive_request(client, self)

                self._requests_count += 1
                if self._verbose:
                    print('%d\t%r' % (request_msg.client_port, request_msg))

                # Give most recently added responders precedence.
                for matcher, args, kwargs in reversed(self._autoresponders):
                    if matcher.matches(request_msg):
                        if self._verbose:
                            print('\tautoresponding')
                        request_msg.reply(*args, **kwargs)
                        break
                else:
                    self._request_q.put(request_msg)
            except socket.error as error:
                if error.errno == errno.EAGAIN:
                    continue
                elif error.errno in (errno.ECONNRESET, errno.EBADF):
                    # We hung up, or the client did.
                    break
                raise

        client.close()

    @contextlib.contextmanager
    def _unlock(self):
        """Temporarily release the lock."""
        self._lock.release()
        try:
            yield
        finally:
            self._lock.acquire()

    def __iter__(self):
        return self

    def next(self):
        request = self.receives()
        if request is None:
            # Server stopped.
            raise StopIteration()
        return request

    __next__ = next

    def __repr__(self):
        return 'MockupDB(%s, %s)' % self._address


def bind_socket(address):
    """Takes (host, port) and returns (socket_object, (host, port)).

    If the passed-in port is None, bind an unused port and return it.
    """
    host, port = address
    for res in set(socket.getaddrinfo(host, port, socket.AF_INET,
                                      socket.SOCK_STREAM, 0,
                                      socket.AI_PASSIVE)):

        family, socktype, proto, _, sock_addr = res
        sock = socket.socket(family, socktype, proto)
        if os.name != 'nt':
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Automatic port allocation with port=None.
        sock.bind(sock_addr)
        sock.listen(128)
        bound_port = sock.getsockname()[1]
        return sock, (host, bound_port)

    raise socket.error('could not bind socket')


OPCODES = {OP_QUERY: OpQuery,
           OP_INSERT: OpInsert,
           OP_UPDATE: OpUpdate,
           OP_DELETE: OpDelete,
           OP_GET_MORE: OpGetMore,
           OP_KILL_CURSORS: OpKillCursors}


def mock_server_receive_request(client, server):
    """Take a client socket and return a Request."""
    header = mock_server_receive(client, 16)
    length = _UNPACK_INT(header[:4])[0]
    request_id = _UNPACK_INT(header[4:8])[0]
    opcode = _UNPACK_INT(header[12:])[0]
    msg_bytes = mock_server_receive(client, length - 16)
    if opcode not in OPCODES:
        raise NotImplementedError("Don't know how to unpack opcode %d yet"
                                  % opcode)
    return OPCODES[opcode].unpack(msg_bytes, client, server, request_id)


def mock_server_receive(sock, length):
    """Receive `length` bytes from a socket object."""
    msg = b''
    while length:
        chunk = sock.recv(length)
        if chunk == b'':
            raise socket.error(errno.ECONNRESET, 'closed')

        length -= len(chunk)
        msg += chunk

    return msg


def make_docs(*args, **kwargs):
    """Make the documents for a `Request` or `OpReply`.

    Takes a variety of argument styles, returns a list of dicts.

    Used by `make_request` and `make_reply`, which are in turn used by
    `MockupDB.receives`, `Request.replies`, and so on. See examples in
    tutorial.
    """
    # Error we might raise.
    value_err = ValueError("Can't interpret args %r, %r" % (args, kwargs))
    if not args and not kwargs:
        return []

    if not args:
        # OpReply(ok=1, ismaster=True).
        return [kwargs]

    if isinstance(args[0], (int, float, bool)):
        # server.receives().ok(0, err='uh oh').
        if args[1:]:
            raise value_err
        doc = {'ok': args[0]}
        doc.update(kwargs)
        return [doc]

    if isinstance(args[0], (list, tuple)):
        # Send a batch: OpReply([{'a': 1}, {'a': 2}]).
        if not all(isinstance(doc, (OpReply, collections.Mapping))
                   for doc in args[0]):
            raise TypeError('each doc must be a dict')
        if kwargs:
            raise value_err
        return list(args[0])

    if isinstance(args[0], string_type):
        # OpReply('ismaster', me='a.com').
        if args[1:]:
            raise value_err
        doc = {args[0]: 1}
        doc.update(kwargs)
        return [doc]

    if kwargs:
        raise value_err

    # Send a batch as varargs: OpReply({'a': 1}, {'a': 2}).
    if not all(isinstance(doc, (OpReply, collections.Mapping)) for doc in args):
        raise TypeError('each doc must be a dict')

    return args


# TODO: rename to make_request?
def make_prototype_request(*args, **kwargs):
    """Make a prototype Request for a Matcher."""
    if args and inspect.isclass(args[0]) and issubclass(args[0], Request):
        request_cls, arg_list = args[0], args[1:]
        return request_cls(*arg_list, **kwargs)
    if args and isinstance(args[0], Request):
        if args[1:] or kwargs:
            raise ValueError("Can't interpret args %r, %r" % (args, kwargs))
        return args[0]

    # Match any opcode.
    return Request(*args, **kwargs)


# TODO: remove?
def make_request(*args, **kwargs):
    """Make a Request from a request spec, a Command by default.

    See examples in tutorial.
    """
    if not args:
        return Command(**kwargs)
    if inspect.isclass(args[0]) and issubclass(args[0], Request):
        request_cls, arg_list = args[0], args[1:]
        return request_cls(*arg_list, **kwargs)
    if isinstance(args[0], Request):
        if args[1:] or kwargs:
            raise ValueError("Can't interpret args %r, %r" % (args, kwargs))
        return args[0]

    return Command(*args, **kwargs)


def make_reply(*args, **kwargs):
    """Make an `OpReply` from a reply pattern. See examples in tutorial."""
    # Error we might raise.
    if args and isinstance(args[0], OpReply):
        if args[1:] or kwargs:
            raise ValueError("Can't interpret args %r, %r" % (args, kwargs))
        return args[0]

    return OpReply(*args, **kwargs)


def interactive_server(port=27017, verbose=True):
    """A `MockupDB` that the mongo shell can connect to.

    Call `~.MockupDB.run` on the returned server, and clean it up with
    `~.MockupDB.stop`.
    """
    server = MockupDB(port=port,
                         verbose=verbose,
                         request_timeout=int(1e6))
    server.autoresponds({})
    server.autoresponds(OpQuery, {'a': 1}, {'a': 2})
    server.autoresponds('ismaster')
    server.autoresponds('isMaster', ismaster=True, setName='MockupDB')
    server.autoresponds('whatsmyuri', you='localhost:12345')
    server.autoresponds({'getLog': 'startupWarnings'},
                        log=['hello from MockupDB!'])
    server.autoresponds('replSetGetStatus', ok=0)
    return server


def main():
    """Start an interactive `MockupDB`.

    Use like ``python -m test.mock_mongodb``.
    """
    from optparse import OptionParser
    parser = OptionParser('Start mock MongoDB server')
    parser.add_option('-p', '--port', dest='port', default=27017,
                      help='port on which mock mongod listens')
    parser.add_option('-q', '--quiet',
                      action='store_false', dest='verbose', default=True,
                      help="don't print messages to stdout")

    options, cmdline_args = parser.parse_args()
    if cmdline_args:
        parser.error('Unrecognized argument(s): %s' % ' '.join(cmdline_args))

    server = interactive_server(port=options.port, verbose=options.verbose)
    try:
        server.run()
        print('Listening on port %d' % server.port)
        time.sleep(1e6)
    except KeyboardInterrupt:
        server.stop()

if __name__ == '__main__':
    main()
