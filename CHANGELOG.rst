.. :changelog:

Changelog
=========

Next Release
------------

MockupDB no longer supports Python 2.6 or Python 3.3.

New method ``MockupDB.append_responder`` to add an autoresponder of last resort.

Fix a bug in ``interactive_server`` with ``all_ok=True``. It had returned an
empty isMaster response, causing drivers to throw errors like "Server at
localhost:27017 reports wire version 0, but this version of PyMongo requires at
least 2 (MongoDB 2.6)."

Parse OP_MSGs with any number of sections in any order. This allows write
commands from the mongo shell, which sends sections in the opposite order from
drivers.

1.7.0 (2018-12-02)
------------------

Improve datetime support in match expressions. Python datetimes have microsecond
precision but BSON only has milliseconds, so expressions like this always
failed::

  server.receives(Command('foo', when=datetime(2018, 12, 1, 6, 6, 6, 12345)))

Now, the matching logic has been rewritten to recurse through arrays and
subdocuments, comparing them value by value. It compares datetime values with
only millisecond precision.

1.6.0 (2018-11-16)
------------------

Remove vendored BSON library. Instead, require PyMongo and use its BSON library.
This avoids surprising problems where a BSON type created with PyMongo does not
appear equal to one created with MockupDB, and it avoids the occasional need to
update the vendored code to support new BSON features.

1.5.0 (2018-11-02)
------------------

Support for Unix domain paths with ``uds_path`` parameter.

The ``interactive_server()`` function now prepares the server to autorespond to
the ``getFreeMonitoringStatus`` command from the mongo shell.

1.4.1 (2018-06-30)
------------------

Fix an inadvertent dependency on PyMongo, which broke the docs build.

1.4.0 (2018-06-29)
------------------

Support, and expect, OP_MSG requests from clients. Thanks to Shane Harvey for
the contribution.

Update vendored bson library from PyMongo. Support the Decimal128 BSON type. Fix
Matcher so it equates BSON objects from PyMongo like ``ObjectId(...)`` with
equivalent objects created from MockupDB's vendored bson library.

1.3.0 (2018-02-19)
------------------

Support Windows. Log a traceback if a bad client request causes an assert. Fix
SSL. Make errors less likely on shutdown. Enable testing on Travis and Appveyor.
Fix doctests and interactive server for modern MongoDB protocol.

1.2.1 (2017-12-06)
------------------

Set minWireVersion to 0, not to 2. I had been wrong about MongoDB 3.6's wire
version range: it's actually 0 to 6. MockupDB now reports the same wire version
range as MongoDB 3.6 by default.

1.2.0 (2017-09-22)
------------------

Update for MongoDB 3.6: report minWireVersion 2 and maxWireVersion 6 by default.

1.1.3 (2017-04-23)
------------------

Avoid rare RuntimeError in close(), if a client thread shuts down a socket as
MockupDB iterates its list of sockets.

1.1.2 (2016-08-23)
------------------

Properly detect closed sockets so ``MockupDB.stop()`` doesn't take 10 seconds
per connection. Thanks to Sean Purcell.

1.1.1 (2016-08-01)
------------------

Don't use "client" as a keyword arg for ``Request``, it conflicts with the
actual "client" field in drivers' new handshake protocol.

1.1.0 (2016-02-11)
------------------

Add cursor_id property to OpGetMore, and ssl parameter to interactive_server.

1.0.3 (2015-09-12)
------------------

``MockupDB(auto_ismaster=True)`` had just responded ``{"ok": 1}``, but this
isn't enough to convince PyMongo 3 it's talking to a valid standalone,
so auto-respond ``{"ok": 1, "ismaster": True}``.

1.0.2 (2015-09-11)
------------------

Restore Request.assert_matches method, used in pymongo-mockup-tests.

1.0.1 (2015-09-11)
------------------

Allow co-installation with PyMongo.

1.0.0 (2015-09-10)
------------------

First release.

0.1.0 (2015-02-25)
------------------

Development begun.
