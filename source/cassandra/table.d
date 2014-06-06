module cassandra.table;

import cassandra.cql.result;
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
		auto column_names = fieldNames!T[].join(", ");
		auto column_values = valueStrings(fields)[].join(", ");
		auto str = format("INSERT INTO %s (%s) VALUES (%s)", m_name, column_names, column_values);
		m_keyspace.query(str, Consistency.any);
	}

	void truncate()
	{
		m_keyspace.query("TRUNCATE "~m_name);
	}

	CassandraResult select(string expr = null/*, order, limit*/)
	{
		string query = "SELECT * FROM "~m_name;
		if (expr) query ~= "WHERE "~expr;
		return m_keyspace.query(query);
	}

	void createIndex(string column, string custom_class = null)
	{
		m_keyspace.query(format("CREATE %sINDEX ON %s.%s (%s) %s",
			custom_class.length ? "CUSTOM " : "", m_keyspace.name, m_name, column,
			custom_class.length ? " USING " ~ custom_class : ""));
	}
}

struct ColumnDescription {
	string name;
	Option type;
}

private string[T.tupleof.length] fieldNames(T)()
{
	string[T.tupleof.length] ret;
	foreach (i, FT; typeof(T.tupleof))
		ret[i] = __traits(identifier, T.tupleof[i]);
	return ret;
}
private string[T.tupleof.length] valueStrings(T)(T fields)
{
	string[T.tupleof.length] ret;
	foreach (i, F; fields.tupleof)
		ret[i] = toCQLString(F);
	return ret;
}