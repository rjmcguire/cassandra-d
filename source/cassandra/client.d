module cassandra.client;

import cassandra.cql;
import cassandra.schema;

class CassandraClient {
	private {
		string m_host;
		ushort m_port;
		version (Have_vibe_d) {
			import vibe.core.connectionpool : ConnectionPool;
			ConnectionPool!Connection m_connections;
		}
	}

	this (string host, ushort port = Connection.defaultport) {
		m_host = host;
		m_port = port;

		version (Have_vibe_d) {
			m_connections = new ConnectionPool!Connection(&createConnection);
		}
	}

	CassandraSchema getSchema(string schema) {
		return CassandraSchema(this, schema);
	}

	/+CassandraTable getTable(string table) {
		return CassandraTable(this, table);
	}+/

	auto getConnection() {
		return lockConnection();
	}

	version (Have_vibe_d) {
		package auto lockConnection() { return m_connections.lockConnection(); }
	} else {
		package auto lockConnection() { return createConnection(); }
	}

	private Connection createConnection()
	{
		auto ret = new Connection();
		ret.connect(m_host, m_port);
		return ret;
	}
}