module cassandra.schema;

import cassandra.keyspace;
import cassandra.internal.utils;


struct CassandraSchema {
	private {
		CassandraKeyspace m_keyspace;
		string m_name;
	}

	this(CassandraKeyspace keyspace, string schema_name)
	{
		enforceValidIdentifier(schema_name);

		m_keyspace = keyspace;
		m_name = schema_name;
	}

	@property string name() const { return m_name; }
}
