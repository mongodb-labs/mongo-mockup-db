============
Installation
============

MockMongoDB requires PyMongo_. It uses PyMongo's ``bson`` package to encode
and decode MongoDB Wire Protocol message bodies.

At the command line::

    $ easy_install mongo-mockup-db

Or, if you have virtualenvwrapper installed::

    $ mkvirtualenv mongo-mockup-db
    $ pip install mongo-mockup-db

.. _PyMongo: https://pypi.python.org/pypi/pymongo/
