.. :changelog:

Changelog
=========

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
