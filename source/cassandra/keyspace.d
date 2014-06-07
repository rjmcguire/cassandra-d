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

	CassandraTable createTable(TABLE, ATTRIBUTES...)(string name, PrimaryKeyAttribute primary_key, ATTRIBUTES attributes)
	{
		//assert(isIdentifier(name));
		auto stmt = appender!string();
		stmt ~= "CREATE TABLE "~name~" (";
		foreach (i, T; typeof(TABLE.tupleof)) {
			stmt ~= "\n  " ~ __traits(identifier, TABLE.tupleof[i]) ~ " ";
			stmt ~= bestCassandraType!T ~ ",";
		}

		stmt ~= "\n  PRIMARY KEY (";
		if (primary_key.partitionKey.length > 1) {
			stmt ~= "(" ~ primary_key.partitionKey.join(", ") ~ ")";
			if (primary_key.clusteringColumns.length)
				stmt ~= ", " ~ primary_key.clusteringColumns.join(", ");
		} else stmt ~= (primary_key.partitionKey ~ primary_key.clusteringColumns).join(", ");
		stmt ~= ")\n)";

		// TODO: support WITH attributes!

		query(stmt.data);
		return CassandraTable(this, name);
	}

	CassandraResult query(string q, Consistency consistency = Consistency.one)
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


/+/**
	Attribute for declaring the primary key of a table.

	Params:
		column_names = The names of the columns that make up the primary key.
			This can be one or more columns, of which the first one will be
			the partition key.
		partition_key = An array of column names used as a composite
			partition key.
		clustering_columns = Additional columns to include for the primary key
			to determine the clustering order.
*/
PrimaryKeyAttribute primaryKey(string[] column_names...)
{
	assert(column_names.length >= 1);
	return PrimaryKeyAttribute([column_names[0]], column_names[1 .. $]);
}
/// ditto
PrimaryKeyAttribute primaryKey(string[] partition_key, string[] clustering_columns...)
{
	assert(partition_key.length >= 1);
	return PrimaryKeyAttribute(partition_key, clustering_columns);
}

/** Examples translated from the CQL documentation

	See also $(LINK http://www.datastax.com/documentation/cql/3.0/cql/cql_reference/create_table_r.html).
*/
unittest {
	import std.uuid;

	// single primary key: user_name
	@primaryKey("user_name")
	struct users {
		string user_name;
		string password;
		string gender;
		string session_token;
		string state;
		long birth_year;
	}

	// compound primary key: empID and deptID
	@primaryKey("empID", "deptID")
	struct emp {
		int empID;
		int deptID;
		string first_name;
		string last_name;
	}

	// composite partition key: block_id and breed
	// additional clustering columne: color and short_hair
	@primaryKey(["block_id", "breed"], "color", "short_hair")
	struct Cats {
		UUID block_id;
		string breed;
		string color;
		bool short_hair;
	}
}+/

struct PrimaryKeyAttribute {
	string[] partitionKey;
	string[] clusteringColumns;
}