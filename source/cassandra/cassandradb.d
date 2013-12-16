module cassandra.cassandradb;

public import cassandra.client;

CassandraClient connectCassandraDB(string host, ushort port) {
	return new CassandraClient(host, port);
}

CassandraClient connectCassandraDB(string host) {
	return new CassandraClient(host);
}
