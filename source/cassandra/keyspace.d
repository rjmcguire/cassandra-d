module cassandra.keyspace;

import cassandra.client;
import cassandra.cql;
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

	Connection.Result query(string q, Consistency consistency = Consistency.ANY)
	{
		auto conn = m_client.lockConnection();
		conn.useKeyspace(m_name);
		return conn.query(q, consistency);
	}

	Connection.Result select(string q, Consistency consistency = Consistency.QUORUM) {
		auto conn = m_client.lockConnection();
		conn.useKeyspace(m_name);
		return conn.select(q, consistency);
	}

	CassandraSchema getSchema(string schema) {
		return CassandraSchema(this, schema);
	}

	CassandraTable getTable(string table) {
		return CassandraTable(this, table);
	}
}
