module cassandra.cassandradb;

import cassandra.client;

CassandraClient connectCassandraDB(string host, ushort port) {
	return new CassandraClient(host, port);
}
