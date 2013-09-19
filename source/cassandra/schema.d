module cassandra.schema;

import cassandra.cql;
import cassandra.client;

struct CassandraSchema {
	CassandraClient client_;
	string schema_;
	this(CassandraClient client, string schema) {
		client_ = client;
		schema_ = schema_;

		auto conn = client_.lockConnection;
		conn.query("use clearformat", Consistency.ANY);

	}

	@property
	string schema() { return schema_; }

	auto describeTable(string table) {
		auto conn = client_.lockConnection;
		auto res = conn.select("SELECT * FROM "~ table);
		Option[string] ret;
		foreach (cs; res.metadata.column_specs) {
			ret[cs.name] = cs.type;
		}
		return ret;
	}


}