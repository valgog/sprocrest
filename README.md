*This is a work in progress project*

RESTful interface for the database stored procedures
====================================================

A very thin service layer, that would expose database stored procedures as RESTful service.

The following features are on the roadmap for now:
* detect and expose all stored procedures as RESTful API calls,
* perform automatic mapping of the database types to and from JSON,
* make it possible to do parallel or sequential access to the database shards,
* make it possible to annotate stored procedures so that it is clear how to do sharding and result aggregation
