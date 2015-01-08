[![Build Status](https://travis-ci.org/zalando/sprocrest.svg)](https://travis-ci.org/zalando/sprocrest)

*This is a work in progress project*

RESTful interface for the database stored procedures
====================================================

A very thin service layer, that would expose database stored procedures as RESTful service.

The idea
--------

PostgreSQL database provides a very powerful framework for developing parts of business logic in Stored Procedures.
Advanced stored procedure languages are available, starting from the original pl/pgsql and finishing with pl/python and pl/v8 and event pl/R.
Zalando Technology gathered a lot of experience successfully deploying thousands of stored procedures per week on the production servers.
Java developers can easily call stored procedures as if they ware native RPC function calls
(see [Stored Procedure Wrapper](https://github.com/zalando/java-sproc-wrapper) for more details).
Stored procedure deployment process is also completely automated and is transparent for developers thanks to the
[schema based stored procedure API deployment process](http://tech.valgog.com/2012/01/schema-based-versioning-and-deployment.html).
But as in many cases, Java application becomes a mere transport layer between the database and the client. To automate the process of the

So the main idea of this project is to make things even easier and convert PostgreSQL server to a real application server with RESTful API exposed.

Roadmap
-------

The following features are on the roadmap for now:

* detect and expose all stored procedures as RESTful API calls,
* perform automatic mapping of the database types to and from PostgreSQL stored procedures,
* make it possible to do parallel or sequential access to the database shards (set of databases),
* make it possible to annotate stored procedures so that it is clear how to do sharding and result aggregation

Developing
----------

You might need to create a postgres database to get the application tests to run:

    bash bin/bootstrap_database.bash

> :warning: This script will destroy any database or role, created by the user in the local cluster previously.
> Do no use on any database, that is used for anything else then testing this project code.

Then to run the application:

    sbt run

This will start the application on port 9000.

You might also like to leave it in a compile/run loop if you are developing.
From the shell run `$ sbt`, then within the sbt shell:

	> ~run


TODO
----
What things are still to be done:

- [x] load metadata from database catalog
  - [x] load type information
  - [x] load stored procedure signatures
  - [x] periodic reloading of metadata
- [x] call stored procedures
  - [x] return simple and complex types
  - [x] support parameter overloading
  - [x] map and pass simple types
  - [x] map and pass complex types
- [ ] support more than one database (especially database shards)
  - [x] enable to configure more then one database in the config
  - [ ] enable support for DDS (Database Discovery Service) configuration
  - [ ] treat shards as belonging to one merged database
- [ ] merge results from several database shards
- [ ] support stored procedure annotations
  - [ ] sharding strategies
  - [ ] sharded result merging
  - [ ] 2PC
