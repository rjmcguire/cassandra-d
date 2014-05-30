module cassandra.table;

import cassandra.cql.utils;
import cassandra.keyspace;
import cassandra.internal.utils;

import std.algorithm : map;
import std.array : array, join;
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
		auto res = sysks.query(format(`SELECT columnfamily_name FROM schema_columnfamilies WHERE keyspace_name='%s' AND columnfamily_name='%s'`, m_keyspace.name, m_name));
		return !res.empty;
	}

	ColumnDescription[] describe()
	{
		auto res = m_keyspace.query(format("SELECT * FROM %s", m_name));
		return res.metadata.column_specs.map!(cs => ColumnDescription(cs.name, cs.type)).array;
	}

	void insert(T)(T fields)
		if (is(T == struct) || is(T == class))
	{
		auto column_names = [__traits(allMembers, T)].join(", ");
		auto column_values = [__traits(allMembers, T)].map!(m => toCQLString(m)).array.join(", ");
		auto str = format("INSERT INTO %s (%s) VALUES (%s)", m_name, column_names, column_values);
		m_keyspace.query(str, Consistency.any);
	}

	void truncate()
	{
		m_keyspace.query("TRUNCATE "~m_name);
	}
}

struct ColumnDescription {
	string name;
	Option type;
}
