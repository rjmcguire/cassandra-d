module cassandra.cql.result;

public import cassandra.cql.utils;

import cassandra.cql.connection;
import cassandra.internal.utils;
import cassandra.internal.tcpconnection;

import std.array;
import std.bigint;
import std.bitmanip : bitfields;
import std.conv;
import std.exception : enforce;
import std.format : formattedWrite;
import std.range : isOutputRange;
import std.stdint;
import std.string : format;
import std.traits;


struct CassandraResult {
	/*
	 *4.2.5. RESULT
	 *
	 *  The result to a query (QUERY, PREPARE or EXECUTE messages).
	 *
	 *  The first element of the body of a RESULT message is an [int] representing the
	 *  `kind` of result. The rest of the body depends on the kind. The kind can be
	 *  one of:
	 *    0x0001    Void: for results carrying no information.
	 *    0x0002    Rows: for results to select queries, returning a set of rows.
	 *    0x0003    Set_keyspace: the result to a `use` query.
	 *    0x0004    Prepared: result to a PREPARE message.
	 *    0x0005    Schema_change: the result to a schema altering query.
	 *
	 *  The body for each kind (after the [int] kind) is defined below.
	 */
	enum Kind : short {
		void_        = 0x0001,
		rows         = 0x0002,
		setKeyspace  = 0x0003,
		prepared     = 0x0004,
		schemaChange = 0x0005
	}

	/*
	 *4.2.5.5. Schema_change
	 *
	 *  The result to a schema altering query (creation/update/drop of a
	 *  keyspace/table/index). The body (after the kind [int]) is composed of 3
	 *  [string]:
	 *    <change><keyspace><table>
	 *  where:
	 *    - <change> describe the type of change that has occured. It can be one of
	 *      "CREATED", "UPDATED" or "DROPPED".
	 *    - <keyspace> is the name of the affected keyspace or the keyspace of the
	 *      affected table.
	 *    - <table> is the name of the affected table. <table> will be empty (i.e.
	 *      the empty string "") if the change was affecting a keyspace and not a
	 *      table.
	 *
	 *  Note that queries to create and drop an index are considered as change
	 *  updating the table the index is on.
	 */
	enum Change : string {
		created = "CREATED",
		updated = "UPDATED",
		dropped = "DROPPED"
	}

	/*  where:
	 *    - <metadata> is composed of:
	 *        <flags><columns_count><global_table_spec>?<col_spec_1>...<col_spec_n>
	 *      where:
	 *        - <flags> is an [int]. The bits of <flags> provides information on the
	 *          formatting of the remaining informations. A flag is set if the bit
	 *          corresponding to its `mask` is set. Supported flags are, given there
	 *          mask:
	 *            0x0001    Global_tables_spec: if set, only one table spec (keyspace
	 *                      and table name) is provided as <global_table_spec>. If not
	 *                      set, <global_table_spec> is not present.
	 *        - <columns_count> is an [int] representing the number of columns selected
	 *          by the query this result is of. It defines the number of <col_spec_i>
	 *          elements in and the number of element for each row in <rows_content>.
	 *        - <global_table_spec> is present if the Global_tables_spec is set in
	 *          <flags>. If present, it is composed of two [string] representing the
	 *          (unique) keyspace name and table name the columns return are of.
	 */
	struct MetaData {
		int flags; enum GLOBAL_TABLES_SPEC = 0x0001; @property bool hasGlobalTablesSpec() { return flags & MetaData.GLOBAL_TABLES_SPEC ? true : false; }
		int columns_count;
		string[2] global_table_spec;
		ColumnSpecification[] column_specs;
	}

	private {
		Change m_lastChange;
		string m_currentKeyspace;
		string m_currentTable;
		FrameHeader m_fh;
		Kind m_kind;
		TCPConnection m_sock;
		int* m_counterP;
		@property ref int m_counter() { return *m_counterP; }
		MetaData m_metadata;
		int m_rowCount;
		Connection.Lock m_lock;
	}

	this(Connection.Lock lock, FrameHeader fh, TCPConnection sock, ref int counter)
	{
		m_fh = fh;
		m_sock = sock;
		m_counterP = &counter;
		
		int tmp;
		m_kind = cast(Kind)readIntNotNULL(tmp, sock, m_counter);
		
		final switch (m_kind) {
			case Kind.void_: readVoid(); break;
			case Kind.rows:
				readRowMetaData();
				readIntNotNULL(m_rowCount, m_sock, m_counter);
				break;
			case Kind.setKeyspace: readSet_keyspace(); break;
			case Kind.prepared: break; // ignored and handled by PreparedStatement later
			case Kind.schemaChange: readSchema_change(); break;
		}

		if (m_rowCount) {
			assert(lock != Connection.Lock.init);
			m_lock = lock;
		}
	}

	~this()
	{
		while (!empty) dropRow();
	}

	@property Kind kind() { return m_kind; }
	@property string lastchange() { return m_lastChange; }
	@property string keyspace() { return m_currentKeyspace; }
	@property string table() { return m_currentTable; }
	@property MetaData metadata() { return m_metadata; }

	bool empty() const { return m_kind != Kind.rows || m_rowCount == 0; }

	void readRow(ROW)(ref ROW dst)
		if (is(ROW == struct))
	{
		import std.typecons : Nullable;

		assert(!empty);

		foreach (ref col; m_metadata.column_specs) {
			auto tp = col.type.id;
			switch (col.name) {
				default:
					log("Ignoring unknown column %s in result.", col.name);
					break;
				foreach (mname; __traits(allMembers, ROW))
					static if (is(typeof(__traits(getMember, dst, mname) = __traits(getMember, dst, mname)))) {
						case mname:
							try {
								if (!readField(__traits(getMember, dst, mname), tp)) {
									static if (isInstanceOf!(Nullable, typeof(__traits(getMember, dst, mname))))
										__traits(getMember, dst, mname).nullify();
									else __traits(getMember, dst, mname) = __traits(getMember, ROW.init, mname);
								}
							} catch (Exception e) {
								throw new Exception("Failed to read field "~mname~" of type "~to!string(tp)~": "~e.msg);
							}
							break;
					}

			}
		}

		if (!--m_rowCount) m_lock.destroy();
	}

	private bool readField(T)(ref T dst, Option.Type type)
	{
		import std.typecons : Nullable;
		import std.bitmanip : read;
		import std.bigint;
		import std.datetime;
		import std.uuid;

		auto len = readBigEndian!int(m_sock, m_counter);
		if (len < 0) return false;

		static if (isInstanceOf!(Nullable, T)) alias FT = typeof(dst.get());
		else alias FT = T;

		auto buf = new ubyte[len];
		m_sock.read(buf);
		m_counter -= len;

		// TODO:
		//   Option.Type.inet:
		//   Option.Type.list:
		//   Option.Type.map:
		//   Option.Type.set:

		static if (is(FT == bool)) {
			enforce(type == Option.Type.boolean, "Expected boolean");
			dst = buf[0] != 0;
		} else static if (is(FT == int)) {
			enforce(type == Option.Type.int_, "Expected 32-bit integer");
			dst = read!int(buf);
		} else static if (is(FT == long)) {
			enforce(type == Option.Type.bigInt, "Expected 64-bit integer");
			dst = read!long(buf);
		} else static if (is(FT : const(string))) {
			with (Option.Type)
				enforce(type == varChar || type == text || type == ascii, "Expected string");
			dst = cast(FT)buf;
		} else static if (is(FT : const(ubyte)[])) {
			//enforce(type == Option.Type.blob, "Expected binary blob");
			dst = cast(FT)buf;
		} else static if (is(FT == float)) {
			enforce(type == Option.Type.float_, "Expected 32-bit float");
			dst = read!float(buf);
		} else static if (is(FT == double)) {
			enforce(type == Option.Type.double_, "Expected 64-bit float");
			dst = read!double(buf);
		} else static if (is(FT == SysTime)) {
			enforce(type == Option.Type.timestamp, "Expected timestamp");
			enum unix_base = unixTimeToStdTime(0);
			dst = SysTime(buf.read!long * 10_000 + unix_base, UTC());
		} else static if (is(FT == UUID)) {
			with (Option.Type)
				enforce(type == uuid || type == timeUUID, "Expected string");
			dst = UUID(buf[0 .. 16]);
		} else static if (is(FT == BigInt)) {
			enforce(type == Option.Type.varInt, "Expected timestamp");
			readBigInt(dst, buf);
		} else static if (is(FT == Decimal)) {
			enforce(type == Option.Type.decimal, "Expected decimal number");
			dst.exponent = read!int(buf);
			readBigInt(dst.number, buf);
		} //else static assert(false, "Unsupported result type: "~FT.stringof);

		return true;
	}

	void dropRow()
	{
		readRowContent();
		if (!--m_rowCount) m_lock.destroy();
	}

	/*
	 *4.2.5.1. Void
	 *
	 *  The rest of the body for a Void result is empty. It indicates that a query was
	 *  successful without providing more information.
	 */
	private void readVoid() { assert(m_kind == Kind.void_); }

	/*
	 *4.2.5.2. Rows
	 *
	 *  Indicates a set of rows. The rest of body of a Rows result is:
	 *    <metadata><rows_count><rows_content>
	 */
	private void readRowMetaData()
	{
		assert(m_kind == Kind.rows || m_kind == Kind.prepared);
		auto md = MetaData();
		md.flags.readIntNotNULL(m_sock, m_counter);
		md.columns_count.readIntNotNULL(m_sock, m_counter);
		if (md.flags & MetaData.GLOBAL_TABLES_SPEC) {
			md.global_table_spec[0] = readShortString(m_sock, m_counter);
			md.global_table_spec[1] = readShortString(m_sock, m_counter);
		}
		md.column_specs = readColumnSpecifications(md.flags & MetaData.GLOBAL_TABLES_SPEC, md.columns_count);
		log("got spec: ", md);
		m_metadata = md;
	}

	/*       - <col_spec_i> specifies the columns returned in the query. There is
	 *          <column_count> such column specification that are composed of:
	 *            (<ksname><tablename>)?<column_name><type>
	 *          The initial <ksname> and <tablename> are two [string] are only present
	 *          if the Global_tables_spec flag is not set. The <column_name> is a
	 *          [string] and <type> is an [option] that correspond to the column name
	 *          and type. The option for <type> is either a native type (see below),
	 *          in which case the option has no value, or a 'custom' type, in which
	 *          case the value is a [string] representing the full qualified class
	 *          name of the type represented. Valid option ids are:
	 *            0x0000    Custom: the value is a [string], see above.
	 *            0x0001    Ascii
	 *            0x0002    Bigint
	 *            0x0003    Blob
	 *            0x0004    Boolean
	 *            0x0005    Counter
	 *            0x0006    Decimal
	 *            0x0007    Double
	 *            0x0008    Float
	 *            0x0009    Int
	 *            0x000A    Text
	 *            0x000B    Timestamp
	 *            0x000C    Uuid
	 *            0x000D    Varchar
	 *            0x000E    Varint
	 *            0x000F    Timeuuid
	 *            0x0010    Inet
	 *            0x0020    List: the value is an [option], representing the type
	 *                            of the elements of the list.
	 *            0x0021    Map: the value is two [option], representing the types of the
	 *                           keys and values of the map
	 *            0x0022    Set: the value is an [option], representing the type
	 *                            of the elements of the set
	 */
	private Option* readOption()
	{
		auto ret = new Option();
		ret.id = cast(Option.Type)readShort(m_sock, m_counter);
		final switch (ret.id) {
			case Option.Type.custom: ret.string_value = readShortString(m_sock, m_counter); break;
			case Option.Type.ascii: break;
			case Option.Type.bigInt: break;
			case Option.Type.blob: break;
			case Option.Type.boolean: break;
			case Option.Type.counter: break;
			case Option.Type.decimal: break;
			case Option.Type.double_: break;
			case Option.Type.float_: break;
			case Option.Type.int_: break;
			case Option.Type.text: break;
			case Option.Type.timestamp: break;
			case Option.Type.uuid: break;
			case Option.Type.varChar: break;
			case Option.Type.varInt: break;
			case Option.Type.timeUUID: break;
			case Option.Type.inet: break;
			case Option.Type.list: ret.option_value = readOption(); break;
			case Option.Type.map:
				ret.key_values_option_value[0] = readOption();
				ret.key_values_option_value[1] = readOption();
				break;
			case Option.Type.set: ret.option_value = readOption(); break;
		}
		return ret;
	}

	struct ColumnSpecification {
		string ksname;
		string tablename;

		string name;
		Option type;
	}

	string toString() const { return format("Result(%s)", m_kind); }

	private auto readColumnSpecification(bool hasGlobalTablesSpec) {
		ColumnSpecification ret;
		if (!hasGlobalTablesSpec) {
			ret.ksname = readShortString(m_sock, m_counter);
			ret.tablename = readShortString(m_sock, m_counter);
		}
		ret.name = readShortString(m_sock, m_counter);
		ret.type = *readOption();
		return ret;
	}
	private auto readColumnSpecifications(bool hasGlobalTablesSpec, int column_count) {
		ColumnSpecification[] ret;
		for (int i=0; i<column_count; i++) {
			ret ~= readColumnSpecification(hasGlobalTablesSpec);
		}
		return ret;
	}

	/**    - <rows_count> is an [int] representing the number of rows present in this
	 *      result. Those rows are serialized in the <rows_content> part.
	 *    - <rows_content> is composed of <row_1>...<row_m> where m is <rows_count>.
	 *      Each <row_i> is composed of <value_1>...<value_n> where n is
	 *      <columns_count> and where <value_j> is a [bytes] representing the value
	 *      returned for the jth column of the ith row. In other words, <rows_content>
	 *      is composed of (<rows_count> * <columns_count>) [bytes].
	 */
	private auto readRowContent()
	{
		ubyte[][] ret;
		foreach (i; 0 .. m_metadata.columns_count) {
			//log("reading index[%d], %s", i, md.column_specs[i]);
			final switch (m_metadata.column_specs[i].type.id) {
				case Option.Type.custom:
					log("warning column %s has custom type", m_metadata.column_specs[i].name);
					ret ~= readIntBytes(m_sock, m_counter);
					break;
				case Option.Type.counter:
					ret ~= readIntBytes(m_sock, m_counter);
					throw new Exception("Read Counter Type has not been checked this is what we got: "~ cast(string)ret[$-1]);
					//break;
				case Option.Type.decimal:
					auto twobytes = readRawBytes(m_sock, m_counter, 2);
					if (twobytes == [0xff,0xff]) {
						twobytes = readRawBytes(m_sock, m_counter, 2);
						ret ~= null;
						break;
					}
					if (twobytes[0]==0x01 && twobytes[1]==0x01) {
						ret ~= readIntBytes(m_sock, m_counter);
					} else {
						auto writer = appender!string();
						formattedWrite(writer, "%s", twobytes);
						throw new Exception("New kind of decimal encountered"~ writer.data);
					}
					break;
				case Option.Type.boolean:
					ret ~= readRawBytes(m_sock, m_counter, int.sizeof);
					break;
				case Option.Type.ascii:
				case Option.Type.bigInt:
				case Option.Type.blob:
				case Option.Type.double_:
				case Option.Type.float_:
				case Option.Type.int_:
				case Option.Type.text:
				case Option.Type.timestamp:
				case Option.Type.uuid:
				case Option.Type.varChar:
				case Option.Type.varInt:
				case Option.Type.timeUUID:
				case Option.Type.inet:
				case Option.Type.list:
				case Option.Type.map:
				case Option.Type.set:
					ret ~= readIntBytes(m_sock, m_counter);
					break;
			}
		}
		return ret;
	}

	/**4.2.5.3. Set_keyspace
	 *
	 *  The result to a `use` query. The body (after the kind [int]) is a single
	 *  [string] indicating the name of the keyspace that has been set.
	 */
	private string readSet_keyspace() {
		assert(m_kind is Kind.setKeyspace);
		return readShortString(m_sock, m_counter);
	}

	private void readSchema_change()
	{
		assert(m_kind is Kind.schemaChange);
		m_lastChange = cast(Change)readShortString(m_sock, m_counter);
		m_currentKeyspace = readShortString(m_sock, m_counter);
		m_currentTable = readShortString(m_sock, m_counter);
	}
}

struct PreparedStatement {
	private {
		ubyte[] m_id;
		Consistency m_consistency = Consistency.any;
	}

	this(ref CassandraResult result)
	{
		if (result.kind != CassandraResult.Kind.prepared)
			throw new Exception("CQLProtocolException, Unknown result type for PREPARE command");

		/*
		 *4.2.5.4. Prepared
		 *
		 *  The result to a PREPARE message. The rest of the body of a Prepared result is:
		 *    <id><metadata>
		 *  where:
		 *    - <id> is [short bytes] representing the prepared query ID.
		 *    - <metadata> is defined exactly as for a Rows RESULT (See section 4.2.5.2).
		 *
		 *  Note that prepared query ID return is global to the node on which the query
		 *  has been prepared. It can be used on any connection to that node and this
		 *  until the node is restarted (after which the query must be reprepared).
		 */
		assert(result.kind is CassandraResult.Kind.prepared);
		log("reading id");
		m_id = readShortBytes(result.m_sock, result.m_counter);
		log("reading metadata");
		/*m_metadata =*/ result.readRowMetaData();
		log("done reading prepared stmt");
	}

	@property const(ubyte)[] id() const { return m_id; }

	@property Consistency consistency() const { return m_consistency; }
	@property void consistency(Consistency value) { m_consistency = value; }

	string toString() const { return format("PreparedStatement(%s)", id.hex); }
}

struct Decimal {
	import std.bigint;
	int exponent; // 10 ^ -exp
	BigInt number;

	string toString() const {
		auto buf = appender!string();
		number.toString(str => buf.put(str), "%d");
		if (exponent <= 0) return buf.data;
		else return buf.data[0 .. $-exponent] ~ "." ~ buf.data[$-exponent .. $];
	}
}

private void readBigInt(ref BigInt dst, in ubyte[] bytes)
{
	auto strbuf = appender!string();
	strbuf.reserve(bytes.length*2 + 3);
	if (bytes[0] < 0x80) {
		strbuf.put("0x");
		foreach (b; bytes) strbuf.formattedWrite("%02X", b);
	} else {
		strbuf.put("-0x");
		foreach (b; bytes) strbuf.formattedWrite("%02X", ~b);
	}
	log(strbuf.data);
	dst = BigInt(strbuf.data);
	if (bytes[0] >= 0x80) dst--; // FIXME: this is not efficient
}