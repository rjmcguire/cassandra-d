module cassandra.keyspace;

import cassandra.client;
import cassandra.cql.result;
import cassandra.internal.utils;
import cassandra.schema;
import cassandra.table;


struct CassandraKeyspace {
	private {
		CassandraClient m_client;
		string m_name;
	}

	this(CassandraClient client, string name)
	{
		enforceValidIdentifier(name);

		m_client = client;
		m_name = name;

		m_client.lockConnection.useKeyspace(name);
	}

	@property string name() const { return m_name; }
	@property inout(CassandraClient) client() inout { return m_client; }

	CassandraSchema getSchema(string schema) { return CassandraSchema(this, schema); }

	CassandraTable getTable(string table) { return CassandraTable(this, table); }

	CassandraResult query(string q, Consistency consistency = Consistency.any)
	{
		auto conn = m_client.lockConnection();
		conn.useKeyspace(m_name);
		return conn.query(conn, q, consistency);
	}

	PreparedStatement prepare(string q)
	{
		auto conn = m_client.lockConnection();
		conn.useKeyspace(m_name);
		return conn.prepare(q);
	}

	CassandraResult execute(ARGS...)(PreparedStatement stmt, ARGS args)
	{
		auto conn = m_client.lockConnection();
		// TODO: assert(stmt.keyspace is this);
		return conn.execute(conn, stmt, args);
	}
}
