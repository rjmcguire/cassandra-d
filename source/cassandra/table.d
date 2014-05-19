module cassandra.table;

import cassandra.cql.utils;
import cassandra.keyspace;
import cassandra.internal.utils;

import std.algorithm : map;
import std.array : array;
import std.string : format;


struct CassandraTable {
	private {
		CassandraKeyspace m_keyspace;
		string m_name;
	}

	this(CassandraKeyspace keyspace, string name)
	{
		enforceValidIdentifier(name);
		m_keyspace = keyspace;
		m_name = name;
	}

	bool exists()
	{
		auto sysks = m_keyspace.client.getKeyspace("system");
		auto res = sysks.select(format(`SELECT columnfamily_name FROM schema_columnfamilies WHERE keyspace_name='%s' AND columnfamily_name='%s'`, m_keyspace.name, m_name));
		return res.rows.length > 0;
	}

	ColumnDescription[] describe()
	{
		auto res = m_keyspace.select(format("SELECT * FROM %s", m_name));
		return res.metadata.column_specs.map!(cs => ColumnDescription(cs.name, cs.type)).array;
	}
}

struct ColumnDescription {
	string name;
	Option type;
}
