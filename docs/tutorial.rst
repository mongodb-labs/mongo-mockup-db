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

You can play with the mock server via ``python -m mock_mongodb``
and connect from the shell, but that is not tremendously interesting.
Better to use it in tests.

We begin by running a :class:`.MockupDB` and connecting to it with
`~pymongo.mongo_client.MongoClient`:

   >>> import sys
   >>> from mockupdb import *
   >>> server = MockupDB()
   >>> port = server.run()  # Returns the TCP port number it listens on.
   >>> from pymongo import MongoClient
   >>> client = MongoClient(server.uri)

When the client connects it calls the "ismaster" command, then blocks until
the server responds. MockupDB receives the "ismaster" command but does not
respond until you tell it to:

   >>> request = server.receives()
   >>> request
   Command({u'ismaster': 1})

We respond:

   >>> request.replies({'ok': 1})

In fact this is the default response, so the next time the client calls
"ismaster" you could just say:

   >>> server.receives().replies()

The `~MockupDB.receives` call blocks until it receives a request from the
client. Responding to each "ismaster" call is tiresome, so tell the client
to send the default response to all ismaster calls:

   >>> server.autoresponds('ismaster')

A call to `~MockupDB.receives` now blocks waiting for some request that
does *not* match "ismaster".

Reply To Legacy Writes
----------------------

Send an OP_INSERT:

   >>> from pymongo.write_concern import WriteConcern
   >>> w0 = WriteConcern(w=0)
   >>> collection = client.db.collection.with_options(write_concern=w0)
   >>> collection.insert_one({'_id': 1})  # doctest: +ELLIPSIS
   <pymongo.results.InsertOneResult object at ...>
   >>> server.receives()
   OpInsert({u'_id': 1})

MockupDB decodes strings in BSON messages from utf-8 to Python unicode
strings, hence the "u" prefix in the output in Python 2.

If PyMongo sends an unacknowledged OP_INSERT it does not block
waiting for you to call `~Request.replies`, but for all other operations it
does. Use `~test.utils.go` to defer PyMongo to a background thread so you
can respond from the main thread:

   >>> # Default write concern is acknowledged.
   >>> collection = client.db.collection
   >>> from mockupdb import go
   >>> future = go(collection.insert_one, {'_id': 2})

Pass a method and its arguments to the `go` function, the same as to
`functools.partial`. It launches `insert_one` on a thread and returns a
handle to its future outcome. Meanwhile, wait for the client's request to
arrive on the main thread:

   >>> server.receives()
   OpInsert({u'_id': 2})
   >>> gle = server.receives()
   >>> gle
   Command({u'getlasterror': 1L})

You could respond with ``{'ok': 1, 'err': None}``, or for convenience:

   >>> gle.replies_to_gle()

The server's getlasterror response unblocks the client, so its future
contains the return value of `~pymongo.collection.Collection.insert_one`,
which is an `~pymongo.results.InsertOneResult`:

   >>> write_result = future()
   >>> write_result  # doctest: +ELLIPSIS
   <pymongo.results.InsertOneResult object at ...>
   >>> write_result.inserted_id
   2

Reply To Write Commands
-----------------------

MockupDB runs the most recently added autoresponders first, and uses the
first that matches. Override the previous "ismaster" responder to upgrade
the wire protocol:

   >>> server.autoresponds('ismaster', maxWireVersion=3)

Test that PyMongo now uses a write command instead of a legacy insert:

   >>> client.close()
   >>> future = go(collection.insert_one, {'_id': 1})
   >>> request = server.receives()
   >>> request  # doctest: +NORMALIZE_WHITESPACE
   Command({u'insert': u'collection',
            u'documents': [{u'_id': 1}],
            u'ordered': True})

To unblock the background thread, send the default reply of ``{ok: 1}}``:

   >>> request.reply()
   >>> assert 1 == future().inserted_id

Simulate a command error:

   >>> future = go(collection.insert_one, {'_id': 1})
   >>> server.receives(insert='collection').command_err(11000, 'eek!')
   >>> future()
   Traceback (most recent call last):
     ...
   DuplicateKeyError: eek!

Or a network error:

   >>> future = go(collection.insert_one, {'_id': 1})
   >>> server.receives(insert='collection').hangup()
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
   AssertionError: expected to receive Command({'commandBar': 1}),
     got Command({u'commandFoo': 1})

Even if the pattern does not match, the request is still popped from the
queue.

If you do not know what order you need to accept requests, you can make a
little loop:

   >>> def loop():
   ...     for request in server:
   ...         if Command('ismaster').matches(request):
   ...             request.ok()
   ...         # Match queries most restrictive first.
   ...         elif OpQuery({'a': {'$gt': 1}}).matches(request):
   ...             request.reply()
   ...         elif OpQuery().matches(request):
   ...             request.reply({'a': 1})
   ...         elif Command('break').matches(request):
   ...             request.ok()
   ...             break
   ...
   >>> future = go(loop)
   >>>
   >>> client = MongoClient(server.uri)
   >>> client.db.collection.find_one()
   {u'a': 1}
   >>> client.db.collection.find_one({'a': {'$gt': 1}}) is None
   True
   >>> client.db.command('break')
   {u'ok': 1}
   >>> future()

You can even implement the "shutdown" command:

   >>> def loop():
   ...     for request in server:
   ...         if Command('ismaster').matches(request):
   ...             request.ok()
   ...         elif Command('shutdown').matches(request):
   ...             server.stop()
   ...
   ...     return 'server done'
   ...
   >>> future = go(loop)
   >>> client.db.command('shutdown')
   Traceback (most recent call last):
     ...
   AutoReconnect: connection closed
   >>> future()
   'server done'

To show off a difficult test that MockupDB makes easy, assert that
PyMongo sends a ``writeConcern`` argument if you specify ``w=1``:

   >>> server = MockupDB()
   >>> server.autoresponds('ismaster', maxWireVersion=3)
   >>> port = server.run()
   >>>
   >>> collection = MongoClient(server.uri, w=1).db.collection
   >>> future = go(collection.insert_one, {'_id': 4})
   >>> server.receives({'writeConcern': {'w': 1}}).sends()

... but not by default:

   >>> collection = MongoClient(server.uri).db.collection
   >>> future = go(collection.insert_one, {'_id': 5})
   >>> assert 'writeConcern' not in server.receives().doc

Test Cursor Behavior
--------------------

Test what happens when a query fails:

   >>> cursor = collection.find().batch_size(1)
   >>> future = go(next, cursor)
   >>> server.receives(OpQuery).fail()
   >>> future()
   Traceback (most recent call last):
     ...
   OperationFailure: database error: MockupDB query failure

OP_KILL_CURSORS has historically been hard to test. On a single
server you could count cursors in `serverStatus`_ to know when one dies.
But in a replica set, the count is confounded by replication cursors coming
and going, and it is precisely in replica sets that it is crucial to verify
PyMongo sends OP_KILLCURSORS to the right server.

You can check the cursor is closed by trying getMores on it until the
server returns CursorNotFound. However, if you are in the midst of a
getMore when the asynchronous OP_KILL_CURSORS arrives, the server
logs "Assertion: 16089:Cannot kill active cursor" and leaves it alive. By
sleeping a long time between getMores the test reduces races, but does not
eliminate them, and at the cost of sluggishness.

But with MockupDB you can test OP_KILL_CURSORS easily and reliably.
We start a cursor with its first batch:

   >>> cursor = collection.find().batch_size(1)
   >>> future = go(next, cursor)
   >>> reply = OpReply({'first': 'doc'}, cursor_id=123)
   >>> server.receives(OpQuery).replies(reply)
   >>> future()
   {u'first': u'doc'}
   >>> cursor.alive
   True

The cursor should send OP_KILL_CURSORS if it is garbage-collected:

   >>> del cursor
   >>> import gc
   >>> _ = gc.collect()
   >>> server.receives(OpKillCursors, cursor_ids=[123])
   OpKillCursors([123])

You can simulate normal querying, too:

   >>> cursor = collection.find().batch_size(1)
   >>> future = go(list, cursor)
   >>> documents = [{'_id': 1}, {'foo': 'bar'}, {'beauty': True}]
   >>> server.receives(OpQuery).replies(OpReply(documents[0], cursor_id=123))
   >>> del documents[0]
   >>> num_sent = 1
   >>> while documents:
   ...     getmore = server.receives(OpGetMore)
   ...     num_to_return = getmore.num_to_return
   ...     print('num_to_return %s' % num_to_return)
   ...     batch = documents[:num_to_return]
   ...     del documents[:num_to_return]
   ...     if documents:
   ...         cursor_id = 123
   ...     else:
   ...         cursor_id = 0
   ...     reply = OpReply(batch,
   ...                     cursor_id=cursor_id,
   ...                     starting_from=num_sent)
   ...     getmore.replies(reply)
   ...     num_sent += len(batch)
   ...
   num_to_return 2

Observe a quirk in the wire protocol: MongoDB treats an initial query
with nToReturn of 1 the same as -1 and closes the cursor after the first
batch. To work around this, PyMongo overrides a batch size of 1 and asks
for 2.

At any rate, the loop completes and the cursor receives all documents:

   >>> future()
   [{u'_id': 1}, {u'foo': u'bar'}, {u'beauty': True}]

But this is just a parlor trick. Let us test something serious.

Test Server Discovery And Monitoring
------------------------------------

To test PyMongo's server monitor, make the server a secondary:

   >>> ismaster_reply = OpReply({
   ...     'ismaster': False,
   ...     'secondary': True,
   ...     'setName': 'rs',
   ...     'maxWireVersion': 3,
   ...     'hosts': ['%s:%d' % server.address]})
   >>> server.autoresponds('ismaster', ismaster_reply)

Connect to the replica set and try to insert:

   >>> client = MongoClient(server.uri, replicaSet='rs')
   >>> collection = client.db.collection
   >>> future = go(collection.insert_one, {'_id': 'my id'})

PyMongo should call "ismaster" every 10 milliseconds, more or less:

   >>> starting_count = server.requests_count
   >>> import time
   >>> time.sleep(0.1)
   >>> ismasters_count = server.requests_count - starting_count
   >>> assert 5 < ismasters_count <= 10

Back to normal:

   >>> ismaster_reply.update(ismaster=True, secondary=False)
   >>> server.gets(insert='collection').ok()
   >>> future().inserted_id
   'my id'

.. _PyMongo: https://pypi.python.org/pypi/pymongo/

.. _MongoDB Wire Protocol: http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/

.. _serverStatus: http://docs.mongodb.org/manual/reference/server-status/

.. _collect: https://docs.python.org/2/library/gc.html#gc.collect
