========
Tutorial
========

.. currentmodule:: mockupdb

This tutorial is the primary documentation for the MockupDB project.

I assume some familiarity with PyMongo_ and the `MongoDB Wire Protocol`_.

.. contents::

Introduction
------------

Begin by running a :class:`.MockupDB` and connecting to it with PyMongo's
`~pymongo.mongo_client.MongoClient`:

   >>> from mockupdb import *
   >>> server = MockupDB()
   >>> port = server.run()  # Returns the TCP port number it listens on.
   >>> from pymongo import MongoClient
   >>> client = MongoClient(server.uri, connectTimeoutMS=999999)

When the client connects it calls the "ismaster" command, then blocks until
the server responds. By default it throws an error if the server doesn't
respond in 10 seconds, so set a longer timeout.

MockupDB receives the "ismaster" command but does not respond until you tell it
to:

   >>> request = server.receives()
   >>> request.command_name
   'ismaster'

We respond:

   >>> request.replies({'ok': 1, 'maxWireVersion': 6})
   True

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


Reply To Write Commands
-----------------------

If PyMongo sends an unacknowledged OP_INSERT it does not block
waiting for you to call `~Request.replies`. However, for acknowledged operations
it does block. Use `~test.utils.go` to defer PyMongo to a background thread so
you can respond from the main thread:

   >>> collection = client.db.coll
   >>> from mockupdb import go
   >>> # Default write concern is acknowledged.
   >>> future = go(collection.insert_one, {'_id': 1})

Pass a method and its arguments to the `go` function, the same as to
`functools.partial`. It launches `~pymongo.collection.Collection.insert_one`
on a thread and returns a handle to its future outcome. Meanwhile, wait for the
client's request to arrive on the main thread:

   >>> cmd = server.receives()
   >>> cmd
   OpMsg({"insert": "coll", "ordered": true, "$db": "db", "$readPreference": {"mode": "primary"}, "documents": [{"_id": 1}]}, namespace="db")

(Note how MockupDB renders requests and replies as JSON, not Python.
The chief differences are that "true" and "false" are lower-case, and the order
of keys and values is faithfully shown, even in Python versions with unordered
dicts.)

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
   1

If you don't need the future's return value, you can express this more tersely
with `going`:

   >>> with going(collection.insert_one, {'_id': 1}):
   ...     server.receives().ok()
   True

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
   ...             if server.got(OpMsg('find', 'coll', filter={'a': {'$gt': 1}})):
   ...                 server.reply(cursor={'id': 0, 'firstBatch':[{'a': 2}]})
   ...             elif server.got('break'):
   ...                 server.ok()
   ...                 break
   ...             elif server.got(OpMsg('find', 'coll')):
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

.. _message spec:

Message Specs
-------------

We've seen some examples of ways to specify messages to send, and examples of
ways to assert that a reply matches an expected pattern. Both are "message
specs", a flexible syntax for describing wire protocol messages.

Matching a request
''''''''''''''''''

One of MockupDB's most useful features for testing your application is that it
can assert that your application's requests match a particular pattern:

  >>> client = MongoClient(server.uri)
  >>> future = go(client.db.collection.insert, {'_id': 1})
  >>> # Assert the command name is "insert" and its parameter is "collection".
  >>> request = server.receives(OpMsg('insert', 'collection'))
  >>> request.ok()
  True
  >>> assert future()

If the request did not match, MockupDB would raise an `AssertionError`.

The arguments to `OpMsg` above are an example of a message spec. The
pattern-matching rules are implemented in `Matcher`.
Here are
some more examples.

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

Prohibit a field:

  >>> Matcher({'field': absent})
  Matcher(Request({"field": {"absent": 1}}))
  >>> Matcher({'field': absent}).matches({'field': 1})
  False
  >>> Matcher({'field': absent}).matches({'otherField': 1})
  True

Order matters if you use an OrderedDict:

  >>> doc0 = OrderedDict([('a', 1), ('b', 1)])
  >>> doc1 = OrderedDict([('b', 1), ('a', 1)])
  >>> Matcher(doc0).matches(doc0)
  True
  >>> Matcher(doc0).matches(doc1)
  False

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

Commands in MongoDB 3.6 and later use the OP_MSG wire protocol message.
The command name is matched case-insensitively:

  >>> Matcher(OpMsg('ismaster')).matches(OpMsg('IsMaster'))
  True

You can match properties specific to certain opcodes:

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

Sending a reply
'''''''''''''''

The default reply is ``{'ok': 1}``:

.. code-block:: pycon3

  >>> request = server.receives()
  >>> request.ok()  # Send {'ok': 1}.

You can send additional information with the `~Request.ok` method:

.. code-block:: pycon3

  >>> request.ok(field='value')  # Send {'ok': 1, 'field': 'value'}.

Simulate a server error with `~Request.command_err`:

.. code-block:: pycon3

  >>> request.command_err(code=11000, errmsg='Duplicate key', field='value')

All methods for sending replies parse their arguments with the `make_reply`
internal function. The function interprets its first argument as the "ok" field
value if it is a number, otherwise interprets it as the first field of the reply
document and assumes the value is 1:

  >>> import mockupdb
  >>> mockupdb.make_op_msg_reply()
  OpMsgReply()
  >>> mockupdb.make_op_msg_reply(0)
  OpMsgReply({"ok": 0})
  >>> mockupdb.make_op_msg_reply("foo")
  OpMsgReply({"foo": 1})

You can pass a dict or OrderedDict of fields instead of using keyword arguments.
This is best for fieldnames that are not valid Python identifiers:

  >>> mockupdb.make_op_msg_reply(OrderedDict([('ok', 0), ('$err', 'bad')]))
  OpMsgReply({"ok": 0, "$err": "bad"})

You can customize the OP_REPLY header flags with the "flags" keyword argument:

  >>> r = mockupdb.make_op_msg_reply(OrderedDict([('ok', 0), ('$err', 'bad')]),
  ...                                flags=OP_MSG_FLAGS['checksumPresent'])
  >>> repr(r)
  'OpMsgReply({"ok": 0, "$err": "bad"}, flags=checksumPresent)'

Although these examples call `make_op_msg_reply` explicitly, this is only to
illustrate how replies are specified. Your code will pass these arguments to a
`Request` method like `~Request.replies`.

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
   >>> server.receives(OpMsg('find', 'coll')).command_err()
   True
   >>> future()
   Traceback (most recent call last):
     ...
   OperationFailure: database error: MockupDB command failure

You can simulate normal querying, too:

   >>> cursor = collection.find().batch_size(2)
   >>> future = go(list, cursor)
   >>> documents = [{'_id': 1}, {'x': 2}, {'foo': 'bar'}, {'beauty': True}]
   >>> request = server.receives(OpMsg('find', 'coll'))
   >>> n = request['batchSize']
   >>> request.replies(cursor={'id': 123, 'firstBatch': documents[:n]})
   True
   >>> while True:
   ...    getmore = server.receives(OpMsg('getMore', 123))
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

.. _PyMongo: https://pypi.python.org/pypi/pymongo/

.. _MongoDB Wire Protocol: https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/

.. _serverStatus: http://docs.mongodb.org/manual/reference/server-status/

.. _collect: https://docs.python.org/2/library/gc.html#gc.collect
