module cassandra.client;

public import cassandra.keyspace;
import cassandra.cql;
import cassandra.internal.utils;

import std.string : format;


class CassandraClient {
	private {
		string m_host;
		ushort m_port;
		version (Have_vibe_d) {
			import vibe.core.connectionpool : ConnectionPool;
			ConnectionPool!Connection m_connections;
		} else {
			Connection m_connection;
		}
	}

	this (string host, ushort port = Connection.defaultport) {
		m_host = host;
		m_port = port;

		version (Have_vibe_d) {
			m_connections = new ConnectionPool!Connection(&createConnection);
		} else m_connection = createConnection();
	}

	CassandraKeyspace getKeyspace(string name) {
		return CassandraKeyspace(this, name);
	}

	CassandraKeyspace createKeyspace(string name/*, ...*/) {
		enforceValidIdentifier(name);
		lockConnection().query(format(`CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`, name), Consistency.ANY);
		return getKeyspace(name);
	}

	version (Have_vibe_d) {
		package auto lockConnection() { return m_connections.lockConnection(); }
	} else {
		package auto lockConnection() { return m_connection; }
	}

	private Connection createConnection()
	{
		auto ret = new Connection();
		ret.connect(m_host, m_port);
		return ret;
	}
}