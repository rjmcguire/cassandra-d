module cassandra.db;

import cassandra.cql;
import cassandra.client;

struct CassandraDatabase {
	CassandraClient client_;
	string schema;
}