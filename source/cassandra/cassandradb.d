module cassandra.cassandradb;

public import cassandra.client;

import cassandra.cql.connection;


/**
	Connects to a Cassandra instance.
*/
CassandraClient connectCassandraDB(string host, ushort port = Connection.defaultPort)
{
	return new CassandraClient(host, port);
}


unittest {
	auto cassandra = connectCassandraDB("127.0.0.1", 9042);

	/*auto opts = cassandra.requestOptions();
	foreach (opt, values; opts) {
		log(opt, values);
	}*/

	CassandraKeyspace ks;
	try ks = cassandra.getKeyspace("twissandra");
	catch (Exception e) ks = cassandra.createKeyspace("twissandra");

	static struct UserTableEntry {
		import std.bigint;

		string user_name;
		string password;
		string gender;
		string session_token;
		string state;
		BigInt birth_year;
	}

	try {
		log("CREATE TABLE ");
		auto res = ks.query(`CREATE TABLE users (
				user_name varchar,
				password varchar,
				gender varchar,
				session_token varchar,
				state varchar,
				birth_year bigint,
				PRIMARY KEY (user_name)
			  )`, Consistency.any);
		log("created table %s %s %s %s", res.kind, res.keyspace, res.lastchange, res.table);
	} catch (Exception e) { log(e.msg); }

	try {
		log("INSERT");
		ks.query(`INSERT INTO users
				(user_name, password)
				VALUES ('jsmith', 'ch@ngem3a')`, Consistency.any);
		log("inserted");
	} catch (Exception e) { log(e.msg); assert(false); }

	try {
		log("SELECT");
		auto res = ks.query(`SELECT * FROM users WHERE user_name='jsmith'`, Consistency.one);
		log("select resulted in %s", res.toString());
		import std.typecons : Tuple;
		while (!res.empty) {
			UserTableEntry entry;
			res.readRow(entry);
			log("ROW: %s", entry);
		}
	} catch (Exception e) { log(e.msg); assert(false); }

	static struct AllTypesEntry {
		import std.bigint;
		import std.datetime;
		import std.uuid;

		string user_name;
		long birth_year;
		string ascii_col; //@ascii
		ubyte[] blob_col;
		bool booleant_col;
		bool booleanf_col;
		Decimal decimal_col;
		double double_col;
		float float_col;
		ubyte[] inet_col;
		int int_col;
		string[] list_col;
		string[string] map_col;
		string[] set_col;
		string text_col;
		SysTime timestamp_col;
		UUID uuid_col;
		UUID timeuuid_col;
		BigInt varint_col;
	}

	try {
		log("CREATE TABLE ");
		auto res = ks.query(`CREATE TABLE alltypes (
				user_name varchar,
				birth_year bigint,
				ascii_col ascii,
				blob_col blob,
				booleant_col boolean,
				booleanf_col boolean,
				decimal_col decimal,
				double_col double,
				float_col float,
				inet_col inet,
				int_col int,
				list_col list<varchar>,
				map_col map<varchar,varchar>,
				set_col set<varchar>,
				text_col text,
				timestamp_col timestamp,
				uuid_col uuid,
				timeuuid_col timeuuid,
				varint_col varint,

				PRIMARY KEY (user_name)
			  )`, Consistency.any);
		log("created table %s %s %s %s", res.kind, res.keyspace, res.lastchange, res.table);
	} catch (Exception e) { log(e.msg); }

	try {
		log("INSERT into alltypes");
		ks.query(`INSERT INTO alltypes (user_name,birth_year,ascii_col,blob_col,booleant_col, booleanf_col,decimal_col,double_col,float_col,inet_col,int_col,list_col,map_col,set_col,text_col,timestamp_col,uuid_col,timeuuid_col,varint_col)
				VALUES ('bob@domain.com', 7777777777,
					'someasciitext', 0x2020202020202020202020202020,
					True, False,
					 123.456, 8.5, 9.44, '127.0.0.1', 999,
					['li1','li2','li3'], {'blurg':'blarg'}, { 'kitten', 'cat', 'pet' },
					'some text col value', 'now', aaaaaaaa-eeee-cccc-9876-dddddddddddd,
					 now(),
					-9494949449
					)`, Consistency.any);
		log("inserted");
	} catch (Exception e) { log(e.msg); assert(false); }


	try {
		log("PREPARE INSERT into alltypes");
		auto stmt = ks.prepare(`INSERT INTO alltypes (user_name,birth_year, ascii_col, blob_col, booleant_col, booleanf_col, decimal_col, double_col
				, float_col, inet_col, int_col, list_col, map_col, set_col, text_col, timestamp_col
				, uuid_col, timeuuid_col, varint_col)`
				` VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
		log("prepared stmt: %s", stmt);
		alias long Bigint;
		alias double Double;
		alias float Float;
		alias int InetAddress4;
		alias ubyte[16] InetAddress16;
		alias long Timestamp;

		ks.execute(stmt, "rory", cast(Bigint)1378218642, "mossesf asciiiteeeext", 0x898989898989
			, true, false, cast(long)999, cast(double)8.88, cast(float)7.77, cast(int)2130706433
			, 66666, ["thr","for"], ["key1": "value1"], ["one","two", "three"], "some more text«»"
			, cast(Timestamp)0x0000021212121212, "\xaa\xaa\xaa\xaa\xee\xee\xcc\xcc\x98\x76\xdd\xdd\xdd\xdd\xdd\xdd"
			, "\xb3\x8b\x6d\xb0\x14\xcc\x11\xe3\x81\x03\x9d\x48\x04\xae\x88\xb3", long.max);
	} catch (Exception e) { log(e.msg); assert(false); } // list should be:  [0, 2, 0, 3, 111, 110, 101, 0, 3, 116, 119, 111]

	try {
		log("SELECT from alltypes");
		auto res = ks.query(`SELECT * FROM alltypes`, Consistency.one);
		log("select resulted in %s", res.toString());

		while (!res.empty) {
			AllTypesEntry entry;
			res.readRow(entry);
			log("ROW: %s", entry);
		}
	} catch (Exception e) { log(e.msg); assert(false); }


	//cassandra.close();
	log("done. exiting");
}
