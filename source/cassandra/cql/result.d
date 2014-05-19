module cassandra.cql.result;

public import cassandra.cql.utils;

import std.array;
import std.bitmanip : bitfields;
import std.conv;
import std.exception : enforce;
import std.format : formattedWrite;
import std.range : isOutputRange;
import std.stdint;
import std.traits;

import cassandra.internal.utils;
import cassandra.internal.tcpconnection;



class CassandraResult {
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
		ubyte[][][] m_rows;
	}

	this(FrameHeader fh, TCPConnection sock, ref int counter)
	{
		m_fh = fh;
		m_sock = sock;
		m_counterP = &counter;
		
		int tmp;
		m_kind = cast(Kind)readIntNotNULL(tmp, sock, m_counter);
		
		final switch (m_kind) {
			case Kind.void_: readVoid(); break;
			case Kind.rows: readRows(); break;
			case Kind.setKeyspace: readSet_keyspace(); break;
			case Kind.prepared: readPrepared(); break;
			case Kind.schemaChange: readSchema_change(); break;
		}
	}

	@property Kind kind() { return m_kind; }
	@property string lastchange() { return m_lastChange; }
	@property string keyspace() { return m_currentKeyspace; }
	@property string table() { return m_currentTable; }
	@property MetaData metadata() { return m_metadata; }
	@property auto rows() { assert(m_kind == Kind.rows); return m_rows; }


	/*
	 *4.2.5.1. Void
	 *
	 *  The rest of the body for a Void result is empty. It indicates that a query was
	 *  successful without providing more information.
	 */
	void readVoid() { assert(m_kind == Kind.void_); }

	/*
	 *4.2.5.2. Rows
	 *
	 *  Indicates a set of rows. The rest of body of a Rows result is:
	 *    <metadata><rows_count><rows_content>
	 */
	auto readRows()
	{
		assert(m_kind == Kind.rows || m_kind == Kind.prepared);
		m_metadata = readRowMetaData();
		// <rows_count> is read within readRowsContent
		m_rows = readRowsContent();
	}

	MetaData readRowMetaData()
	{
		auto md = MetaData();
		md.flags.readIntNotNULL(m_sock, m_counter);
		md.columns_count.readIntNotNULL(m_sock, m_counter);
		if (md.flags & MetaData.GLOBAL_TABLES_SPEC) {
			md.global_table_spec[0] = readShortString(m_sock, m_counter);
			md.global_table_spec[1] = readShortString(m_sock, m_counter);
		}
		md.column_specs = readColumnSpecifications(md.flags & MetaData.GLOBAL_TABLES_SPEC, md.columns_count);
		log("got spec: ", md);
		return md;
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
	Option* readOption()
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
	protected auto readColumnSpecification(bool hasGlobalTablesSpec) {
		ColumnSpecification ret;
		if (!hasGlobalTablesSpec) {
			ret.ksname = readShortString(m_sock, m_counter);
			ret.tablename = readShortString(m_sock, m_counter);
		}
		ret.name = readShortString(m_sock, m_counter);
		ret.type = *readOption();
		return ret;
	}
	protected auto readColumnSpecifications(bool hasGlobalTablesSpec, int column_count) {
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
	protected auto readRowsContent()
	{
		int count;
		auto tmp = readIntNotNULL(count, m_sock, m_counter);
		ReturnType!readRowContent[] ret;
		//log("reading %d rows", count);
		foreach (i; 0 .. count)
			ret ~= readRowContent();
		return ret;
	}
	protected auto readRowContent()
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
	protected string readSet_keyspace() {
		assert(m_kind is Kind.setKeyspace);
		return readShortString(m_sock, m_counter);
	}

	/**
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
	protected void readPrepared() {
		assert(false, "Not a Prepare CassandraResult class type");
	}

	protected void readSchema_change()
	{
		assert(m_kind is Kind.schemaChange);
		m_lastChange = cast(Change)readShortString(m_sock, m_counter);
		m_currentKeyspace = readShortString(m_sock, m_counter);
		m_currentTable = readShortString(m_sock, m_counter);
	}
}

class PreparedStatement : CassandraResult {
	private {
		ubyte[] m_id;
		Consistency m_consistency = Consistency.any;
	}

	this(FrameHeader fh, TCPConnection sock, ref int counter)
	{
		super(fh, sock, counter);

		if (kind != CassandraResult.Kind.prepared)
			throw new Exception("CQLProtocolException, Unknown result type for PREPARE command");
	}

	@property const(ubyte)[] id() const { return m_id; }
	@property ref inout(Consistency) consistency() inout { return m_consistency; }

	/// See section 4.2.5.4.
	protected override void readPrepared()
	{
		assert(m_kind is Kind.prepared);
		m_id = readShortBytes(m_sock, m_counter);
		m_metadata = readRowMetaData();
	}

	override string toString() { return "PreparedStatement("~id.hex~")"; }
}

// some types
static assert(int.sizeof == 4, "int is not 32 bits"~ to!string(int.sizeof));
