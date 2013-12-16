module cassandra.schema;

import cassandra.cql;
import cassandra.keyspace;
import cassandra.internal.utils;


struct CassandraSchema {
	private {
		CassandraKeyspace keyspace_;
		string name_;
	}

	this(CassandraKeyspace keyspace, string schema_name) {
		enforceValidIdentifier(schema_name);

		keyspace_ = keyspace;
		name_ = schema_name;
	}

	@property
	string name() { return name_; }

}
