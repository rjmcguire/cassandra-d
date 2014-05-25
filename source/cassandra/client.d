module cassandra.client;

public import cassandra.keyspace;

import cassandra.cql.connection;
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

	this(string host, ushort port = Connection.defaultPort)
	{
		m_host = host;
		m_port = port;

		version (Have_vibe_d) {
			m_connections = new ConnectionPool!Connection(&createConnection);
		} else m_connection = createConnection();
	}

	CassandraKeyspace getKeyspace(string name) { return CassandraKeyspace(this, name); }

	CassandraKeyspace createKeyspace(string name/*, ...*/)
	{
		enforceValidIdentifier(name);
		auto conn = lockConnection();
		conn.query(conn, format(`CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`, name), Consistency.any);
		return getKeyspace(name);
	}

	version (Have_vibe_d) {
		package auto lockConnection() { return m_connections.lockConnection(); }
	} else {
		package auto lockConnection() { return m_connection; }
	}

	private Connection createConnection()
	{
		auto ret = new Connection(m_host, m_port);
		ret.connect();
		return ret;
	}
}
