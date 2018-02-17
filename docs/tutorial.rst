========
Tutorial
========

.. currentmodule:: mockupdb

This is the primary documentation for the MockupDB project, and its primary
test.

We assume some familiarity with PyMongo_ and the `MongoDB Wire Protocol`_.

.. contents::

Introduction
------------

You can play with the mock server via ``python -m mockupdb`` and connect from
the shell, but that is not tremendously interesting. Better to use it in tests.

We begin by running a :class:`.MockupDB` and connecting to it with PyMongo's
`~pymongo.mongo_client.MongoClient`:

   >>> from mockupdb import *
   >>> server = MockupDB()
   >>> port = server.run()  # Returns the TCP port number it listens on.
   >>> from pymongo import MongoClient
   >>> client = MongoClient(server.uri)

When the client connects it calls the "ismaster" command, then blocks until
the server responds. MockupDB receives the "ismaster" command but does not
respond until you tell it to:

   >>> request = server.receives()
   >>> request.command_name
   'ismaster'

We respond:

   >>> request.replies({'ok': 1, 'maxWireVersion': 6})
   True

In fact this is the default response, so the next time the client calls
"ismaster" you could just say ``server.receives().replies()``.

The `~MockupDB.receives` call blocks until it receives a request from the
client. Responding to each "ismaster" call is tiresome, so tell the client
to send the default response to all ismaster calls:

   >>> responder = server.autoresponds('ismaster', maxWireVersion=6)
   >>> client.admin.command('ismaster') == {'ok': 1, 'maxWireVersion': 6}
   True

A call to `~MockupDB.receives` now blocks waiting for some request that
does *not* match "ismaster".

(Notice that `~Request.replies` returns True. This makes more advanced uses of
`~MockupDB.autoresponds` easier, see the reference document.)

Reply To Legacy Writes
----------------------

Send an unacknowledged OP_INSERT:

   >>> from pymongo.write_concern import WriteConcern
   >>> w0 = WriteConcern(w=0)
   >>> collection = client.db.coll.with_options(write_concern=w0)
   >>> collection.insert_one({'_id': 1})  # doctest: +ELLIPSIS
   <pymongo.results.InsertOneResult object at ...>
   >>> server.receives()
   OpInsert({"_id": 1}, namespace="db.coll")

If PyMongo sends an unacknowledged OP_INSERT it does not block
waiting for you to call `~Request.replies`, but for all other operations it
does. Use `~test.utils.go` to defer PyMongo to a background thread so you
can respond from the main thread:

   >>> # Default write concern is acknowledged.
   >>> collection = client.db.coll
   >>> from mockupdb import go
   >>> future = go(collection.insert_one, {'_id': 2})

Pass a method and its arguments to the `go` function, the same as to
`functools.partial`. It launches `~pymongo.collection.Collection.insert_one`
on a thread and returns a handle to its future outcome. Meanwhile, wait for the
client's request to arrive on the main thread:

   >>> cmd = server.receives()
   >>> cmd
   Command({"insert": "coll", "ordered": true, "documents": [{"_id": 2}]}, namespace="db")

Respond thus:

   >>> cmd.ok()
   True

The server's response unblocks the client, so its future
contains the return value of `~pymongo.collection.Collection.insert_one`,
which is an `~pymongo.results.InsertOneResult`:

   >>> write_result = future()
   >>> write_result  # doctest: +ELLIPSIS
   <pymongo.results.InsertOneResult object at ...>
   >>> write_result.inserted_id
   2

If you don't need the future's return value, you can express this more tersely
with `going`:

   >>> with going(collection.insert_one, {'_id': 3}):
   ...     server.receives().ok()
   True

Reply To Write Commands
-----------------------

MockupDB runs the most recently added autoresponders first, and uses the
first that matches. Override the previous "ismaster" responder to upgrade
the wire protocol:

   >>> responder = server.autoresponds('ismaster', maxWireVersion=6)

Test that PyMongo now uses a write command instead of a legacy insert:

   >>> client.close()
   >>> future = go(collection.insert_one, {'_id': 1})
   >>> request = server.receives()
   >>> request
   Command({"insert": "coll", "ordered": true, "documents": [{"_id": 1}]}, namespace="db")

(Note how MockupDB requests and replies are rendered as JSON, not Python.
This is mainly to show you the *order* of keys and values, which is sometimes
important when testing a driver.)

To unblock the background thread, send the default reply of ``{ok: 1}}``:

   >>> request.reply()
   True
   >>> assert 1 == future().inserted_id

Simulate a command error:

   >>> future = go(collection.insert_one, {'_id': 1})
   >>> server.receives(insert='coll').command_err(11000, 'eek!')
   True
   >>> future()
   Traceback (most recent call last):
     ...
   DuplicateKeyError: eek!

Or a network error:

   >>> future = go(collection.insert_one, {'_id': 1})
   >>> server.receives(insert='coll').hangup()
   True
   >>> future()
   Traceback (most recent call last):
     ...
   AutoReconnect: connection closed

Pattern-Match Requests
----------------------

MockupDB's pattern-matching is useful for testing: you can tell the server
to verify any aspect of the expected client request.

Pass a pattern to `~.MockupDB.receives` to test that the next request
matches the pattern:

   >>> future = go(client.db.command, 'commandFoo')
   >>> request = server.receives('commandBar') # doctest: +NORMALIZE_WHITESPACE
   Traceback (most recent call last):
     ...
   AssertionError: expected to receive Command({"commandBar": 1}),
     got Command({"commandFoo": 1})

Even if the pattern does not match, the request is still popped from the
queue.

If you do not know what order you need to accept requests, you can make a
little loop:

   >>> import traceback
   >>> def loop():
   ...     try:
   ...         while server.running:
   ...             # Match queries most restrictive first.
   ...             if server.got(Command('find', 'coll', filter={'a': {'$gt': 1}})):
   ...                 server.reply(cursor={'id': 0, 'firstBatch':[{'a': 2}]})
   ...             elif server.got('break'):
   ...                 server.ok()
   ...                 break
   ...             elif server.got(Command('find', 'coll')):
   ...                 server.reply(
   ...                     cursor={'id': 0, 'firstBatch':[{'a': 1}, {'a': 2}]})
   ...             else:
   ...                 server.command_err(errmsg='unrecognized request')
   ...     except:
   ...         traceback.print_exc()
   ...         raise
   ...
   >>> future = go(loop)
   >>>
   >>> list(client.db.coll.find())
   [{'a': 1}, {'a': 2}]
   >>> list(client.db.coll.find({'a': {'$gt': 1}}))
   [{'a': 2}]
   >>> client.db.command('break')
   {'ok': 1}
   >>> future()

You can even implement the "shutdown" command:

   >>> def loop():
   ...     try:
   ...         while server.running:
   ...             if server.got('shutdown'):
   ...                 server.stop()  # Hangs up.
   ...             else:
   ...                 server.command_err('unrecognized request')
   ...     except:
   ...         traceback.print_exc()
   ...         raise
   ...
   >>> future = go(loop)
   >>> client.db.command('shutdown')
   Traceback (most recent call last):
     ...
   AutoReconnect: connection closed
   >>> future()
   >>> server.running
   False
   >>> client.close()

To show off a difficult test that MockupDB makes easy, assert that
PyMongo sends a ``writeConcern`` argument if you specify ``w=1``:

   >>> server = MockupDB()
   >>> responder = server.autoresponds('ismaster', maxWireVersion=6)
   >>> port = server.run()
   >>>
   >>> # Specify w=1. This is distinct from the default write concern.
   >>> client = MongoClient(server.uri, w=1)
   >>> collection = client.db.coll
   >>> future = go(collection.insert_one, {'_id': 4})
   >>> server.receives({'writeConcern': {'w': 1}}).sends()
   True
   >>> client.close()

... but not by default:

   >>> # Accept the default write concern.
   >>> client = MongoClient(server.uri)
   >>> collection = client.db.coll
   >>> future = go(collection.insert_one, {'_id': 5})
   >>> assert 'writeConcern' not in server.receives()
   >>> client.close()

Wait For A Request Impatiently
------------------------------

If your test waits for PyMongo to send a request but receives none, it times out
after 10 seconds by default. This way MockupDB ensures that even failing tests
all take finite time.

To abbreviate the wait, pass a timeout in seconds to `~MockupDB.receives`:

   >>> try:
   ...     server.receives(timeout=0.1)
   ... except AssertionError as err:
   ...     print("Error: %s" % err)
   Error: expected to receive Request(), got nothing

Test Cursor Behavior
--------------------

Test what happens when a query fails:

   >>> cursor = collection.find().batch_size(1)
   >>> future = go(next, cursor)
   >>> server.receives(Command('find', 'coll')).fail()
   True
   >>> future()
   Traceback (most recent call last):
     ...
   OperationFailure: database error: MockupDB query failure

You can simulate normal querying, too:

   >>> cursor = collection.find().batch_size(2)
   >>> future = go(list, cursor)
   >>> documents = [{'_id': 1}, {'x': 2}, {'foo': 'bar'}, {'beauty': True}]
   >>> request = server.receives(Command('find', 'coll'))
   >>> n = request['batchSize']
   >>> request.replies(cursor={'id': 123, 'firstBatch': documents[:n]})
   True
   >>> while True:
   ...    getmore = server.receives(Command('getMore', 123))
   ...    n = getmore['batchSize']
   ...    if documents:
   ...        cursor_id = 123
   ...    else:
   ...        cursor_id = 0
   ...    getmore.ok(cursor={'id': cursor_id, 'nextBatch': documents[:n]})
   ...    print('returned %d' % len(documents[:n]))
   ...    del documents[:n]
   ...    if cursor_id == 0:
   ...        break
   True
   returned 2
   True
   returned 2
   True
   returned 0

The loop receives three getMore commands and replies three times (``True`` is
printed each time we call ``getmore.ok``), sending a cursor id of 0 on the last
iteration to tell PyMongo that the cursor is finished. The cursor receives all
documents:

   >>> future()
   [{'_id': 1}, {'x': 2}, {'_id': 1}, {'x': 2}, {'foo': 'bar'}, {'beauty': True}]

But this is just a parlor trick. Let us test something serious.

Test Server Discovery And Monitoring
------------------------------------

To test PyMongo's server monitor, make the server a secondary:

   >>> hosts = [server.address_string]
   >>> secondary_reply = OpReply({
   ...     'ismaster': False,
   ...     'secondary': True,
   ...     'setName': 'rs',
   ...     'hosts': hosts,
   ...     'maxWireVersion': 6})
   >>> responder = server.autoresponds('ismaster', secondary_reply)

Connect to the replica set:

   >>> client = MongoClient(server.uri, replicaSet='rs')
   >>> from mockupdb import wait_until
   >>> wait_until(lambda: server.address in client.secondaries,
   ...            'discover secondary')
   True

Add a primary to the host list:

   >>> primary = MockupDB()
   >>> port = primary.run()
   >>> hosts.append(primary.address_string)
   >>> primary_reply = OpReply({
   ...     'ismaster': True,
   ...     'secondary': False,
   ...     'setName': 'rs',
   ...     'hosts': hosts,
   ...     'maxWireVersion': 6})
   >>> responder = primary.autoresponds('ismaster', primary_reply)

Client discovers it quickly if there's a pending operation:

   >>> with going(client.db.command, 'buildinfo'):
   ...     wait_until(lambda: primary.address == client.primary,
   ...                'discovery primary')
   ...     primary.pop('buildinfo').ok()
   True
   True

Test Server Selection
---------------------

TODO.

.. _PyMongo: https://pypi.python.org/pypi/pymongo/

.. _MongoDB Wire Protocol: http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/

.. _serverStatus: http://docs.mongodb.org/manual/reference/server-status/

.. _collect: https://docs.python.org/2/library/gc.html#gc.collect
