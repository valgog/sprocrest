[![Build Status](https://travis-ci.org/zalando/sprocrest.svg)](https://travis-ci.org/zalando/sprocrest)

*This is a work in progress project*

RESTful interface for the database stored procedures
====================================================

A very thin service layer, that would expose database stored procedures as RESTful service.

The following features are on the roadmap for now:
* detect and expose all stored procedures as RESTful API calls,
* perform automatic mapping of the database types to and from JSON,
* make it possible to do parallel or sequential access to the database shards,
* make it possible to annotate stored procedures so that it is clear how to do sharding and result aggregation


Developing
==========

You might need to create a postgres database to get the app to start:

    createdb sprocrest
    createuser sprocrest

Then to run the application:

    $sbt run

This will start the application on port 9000.

It doesn't do anything yet.

