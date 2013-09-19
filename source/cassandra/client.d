module cassandra.client;

import cassandra.cql;
import cassandra.schema;

import vibe.core.connectionpool : ConnectionPool;

class CassandraClient {
	private {
		ConnectionPool!Connection m_connections;
	}

	this (string host, short port = Connection.defaultport) {
		m_connections = new ConnectionPool!Connection({
			auto ret = new Connection();
			ret.connect(host,port);
			return ret;
			});
	}
	CassandraSchema getSchema(string schema) {
		return CassandraSchema(this, schema);
	}
	/+CassandraTable getTable(string table) {
		return CassandraTable(this, table);
	}+/
	Connection getConnection() {
		return lockConnection();
	}


	package auto lockConnection() { return m_connections.lockConnection(); }
}