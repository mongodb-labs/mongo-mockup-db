.. :changelog:

Changelog
=========

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
