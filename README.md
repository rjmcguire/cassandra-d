cassandra-d
===========

D language cassandra client (currently binary api v1 and v2)

Currently this driver can be used to create / insert / update / delete data in a cassandra datastore.

There are currently no helpers, you can only execute CQL inputting or retrieving data.

#### Works:
- Queries
- Prepared Statements

### Todo:
- UUID stuff
- Authenticators
- Provide helper functions/templates

build the test with:
- cd source/cassandra
- dmd -main -unittest cql.d serialize.d utils.d tcpconnection.d
OR
- dmd -main -unittest cql.d serialize.d utils.d tcpconnection.d -version=CassandraV2
