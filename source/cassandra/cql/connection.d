module cassandra.cql.connection;

public import cassandra.cql.result;

import std.array;
import std.bitmanip : bitfields;
import std.conv;
import std.exception : enforce;
import std.format : formattedWrite;
import std.range : isOutputRange;
import std.stdint;
import std.string : startsWith;
import std.traits;

import cassandra.internal.utils;
import cassandra.internal.tcpconnection;


class Connection {
	version (Have_vibe_d) {
		import vibe.core.connectionpool : LockedConnection;
		alias Lock = LockedConnection!Connection;
	} else {
		Connection m_connection;
		alias Lock = Connection;
	}

	private {
		TCPConnection sock;
		string m_host;
		ushort m_port;
		string m_usedKeyspace;

		int m_counter; // keeps track of data left after each read

		bool m_compressionEnabled;
		bool m_tracingEnabled;
		byte m_streamID;
		FrameHeader.Version m_transportVersion; // this is just the protocol version number
	}

	enum defaultPort = 9042;

	this(string host, ushort port = defaultPort)
	{
		m_host = host;
		m_port = port;
	}

	void connect()
	{
		if (!sock || !sock.connected) {
			log("connecting");
			sock = connectTCP(m_host, m_port);
			log("connected. doing handshake...");
			startup();
			log("handshake completed.");
			m_usedKeyspace = null;
		}
	}

	void close()
	{
		if (m_counter > 0) {
			auto buf = readRawBytes(sock, m_counter, m_counter);
			log("buf:", buf);
		}
		assert(m_counter == 0, "Did not complete reading of stream: "~ to!string(m_counter) ~" bytes left");
		sock.close();
		sock = null;
	}

	void useKeyspace(string name)
	{
		if (name == m_usedKeyspace) return;
		enforceValidIdentifier(name);
		query(Lock.init, `USE `~name, Consistency.any);
		m_usedKeyspace = name;
	}


	//vvvvvvvvvvvvvvvvvvvvv CQL Implementation vvvvvvvvvvvvvvvvvv
	/// Make a FrameHeader corresponding to this Stream
	FrameHeader makeHeader(FrameHeader.OpCode opcode) {
		FrameHeader fh;
		switch (m_transportVersion) {
			case 1: fh.version_ = FrameHeader.Version.V1Request; break;
			case 2: fh.version_ = FrameHeader.Version.V2Request; break;
			default: assert(0, "invalid transport_version");
		}
		fh.compress = m_compressionEnabled;
		fh.trace = m_tracingEnabled;
		fh.streamid = m_streamID;
		fh.opcode = opcode;
		return fh;
	}

	/**4. Messages
	 *
	 *4.1. Requests
	 *
	 *  Note that outside of their normal responses (described below), all requests
	 *  can get an ERROR message (Section 4.2.1) as response.
	 *
	 *4.1.1. STARTUP
	 *
	 *  Initialize the connection. The server will respond by either a READY message
	 *  (in which case the connection is ready for queries) or an AUTHENTICATE message
	 *  (in which case credentials will need to be provided using CREDENTIALS).
	 *
	 *  This must be the first message of the connection, except for OPTIONS that can
	 *  be sent before to find out the options supported by the server. Once the
	 *  connection has been initialized, a client should not send any more STARTUP
	 *  message.
	 *
	 *  The body is a [string map] of options. Possible options are:
	 *    - "CQL_VERSION": the version of CQL to use. This option is mandatory and
	 *      currenty, the only version supported is "3.0.0". Note that this is
	 *      different from the protocol version.
	 *    - "COMPRESSION": the compression algorithm to use for frames (See section 5).
	 *      This is optional, if not specified no compression will be used.
	 */
	 private void startup(string compression_algorithm = "") {
		StringMap data;
		data["CQL_VERSION"] = "3.0.0";
		if (compression_algorithm.length > 0)
			data["COMPRESSION"] = compression_algorithm;

		auto fh = makeHeader(FrameHeader.OpCode.startup);

		auto bytebuf = appender!(ubyte[])();
		bytebuf.append(data);
		fh.length = bytebuf.getIntLength();
		sock.write(fh.bytes);
		sock.write(bytebuf.data);

		fh = readFrameHeader(sock, m_counter);
		if (fh.isAUTHENTICATE) {
			fh = authenticate(fh);
		}
		throwOnError(fh);
	}

	private FrameHeader authenticate(FrameHeader fh) {
		auto authenticatorname = readAuthenticate(fh);
		auto authenticator = getAuthenticator(authenticatorname);
		sendCredentials(authenticator.getCredentials());
		throw new Exception("NotImplementedException Authentication: "~ authenticatorname);
	}


	/**
	 *4.1.2. CREDENTIALS
	 *
	 *  Provides credentials information for the purpose of identification. This
	 *  message comes as a response to an AUTHENTICATE message from the server, but
	 *  can be use later in the communication to change the authentication
	 *  information.
	 *
	 *  The body is a list of key/value informations. It is a [short] n, followed by n
	 *  pair of [string]. These key/value pairs are passed as is to the Cassandra
	 *  IAuthenticator and thus the detail of which informations is needed depends on
	 *  that authenticator.
	 *
	 *  The response to a CREDENTIALS is a READY message (or an ERROR message).
	 */
	private void sendCredentials(StringMap data) {
		auto fh = makeHeader(FrameHeader.OpCode.credentials);
		auto bytebuf = appender!(ubyte[])();
		bytebuf.append(data);
		fh.length = bytebuf.getIntLength;
		sock.write(fh.bytes);
		sock.write(bytebuf.data);
		
		assert(false, "todo: read credentials response");
	}

	/**
	 *4.1.3. OPTIONS
	 *
	 *  Asks the server to return what STARTUP options are supported. The body of an
	 *  OPTIONS message should be empty and the server will respond with a SUPPORTED
	 *  message.
	 */
	StringMultiMap requestOptions() {
		connect();
		auto fh = makeHeader(FrameHeader.OpCode.options);
		sock.write(fh.bytes);
		fh = readFrameHeader(sock, m_counter);
		if (!fh.isSUPPORTED) {
			throw new Exception("CQLProtocolException, Unknown response to OPTIONS request");
		}
		return readSupported(fh);
	}

	 /**
	 *4.1.4. QUERY
	 *
	 *  Performs a CQL query. The body of the message consists of a CQL query as a [long
	 *  string] followed by the [consistency] for the operation.
	 *
	 *  Note that the consistency is ignored by some queries (USE, CREATE, ALTER,
	 *  TRUNCATE, ...).
	 *
	 *  The server will respond to a QUERY message with a RESULT message, the content
	 *  of which depends on the query.
	 */
	CassandraResult query(Connection.Lock lock, string q, Consistency consistency = Consistency.one)
	{
		assert(!q.startsWith("PREPARE"), "use Connection.prepare to issue PREPARE statements.");
		connect();
		auto fh = makeHeader(FrameHeader.OpCode.query);
		auto bytebuf = appender!(ubyte[])();
		log("-----------");
		bytebuf.appendLongString(q);
		//print(bytebuf.data);
		log("-----------");
		bytebuf.append(consistency);
		fh.length = bytebuf.getIntLength;
		sock.write(fh.bytes);
		sock.write(bytebuf.data);

		fh = readFrameHeader(sock, m_counter);
		throwOnError(fh);
		auto ret = CassandraResult(lock, fh, sock, m_counter);
		assert(ret.kind != CassandraResult.Kind.prepared, "use Connection.prepare to issue PREPARE statements.");
		return ret;
	}

	 /**
	 *4.1.5. PREPARE
	 *
	 *  Prepare a query for later execution (through EXECUTE). The body consists of
	 *  the CQL query to prepare as a [long string].
	 *
	 *  The server will respond with a RESULT message with a `prepared` kind (0x0004,
	 *  see Section 4.2.5).
	 */
	PreparedStatement prepare(string q) {
		connect();
		auto fh = makeHeader(FrameHeader.OpCode.prepare);
		auto bytebuf = appender!(ubyte[])();
		log("---------=-");
		bytebuf.appendLongString(q);
		fh.length = bytebuf.getIntLength;
		sock.write(fh.bytes);
		sock.write(bytebuf.data);
		log("---------=-");

		fh = readFrameHeader(sock, m_counter);
		throwOnError(fh);
		if (!fh.isRESULT) {
			throw new Exception("CQLProtocolException, Unknown response to PREPARE command");
		}
		auto result = CassandraResult(Lock.init, fh, sock, m_counter);
		return PreparedStatement(result);
	}

	/**
	 *4.1.6. EXECUTE
	 *
	 *  Executes a prepared query. The body of the message must be:
	 *    <id><n><value_1>....<value_n><consistency>
	 *  where:
	 *    - <id> is the prepared query ID. It's the [short bytes] returned as a
	 *      response to a PREPARE message.
	 *    - <n> is a [short] indicating the number of following values.
	 *    - <value_1>...<value_n> are the [bytes] to use for bound variables in the
	 *      prepared query.
	 *    - <consistency> is the [consistency] level for the operation.
	 *
	 *  Note that the consistency is ignored by some (prepared) queries (USE, CREATE,
	 *  ALTER, TRUNCATE, ...).
	 *
	 *  The response from the server will be a RESULT message.
	 */
	auto execute(Args...)(Connection.Lock lock, PreparedStatement stmt, Args args)
	{
	//private auto execute(Args...)(ubyte[] preparedStatementID, Consistency consistency, Args args) {
		connect();
		auto fh = makeHeader(FrameHeader.OpCode.execute);
		auto bytebuf = appender!(ubyte[])();
		log("-----=----=-");
		bytebuf.appendShortBytes(stmt.id);
		assert(args.length < short.max);
		bytebuf.append(cast(short)args.length);
		foreach (arg; args) {
			bytebuf.appendIntBytes(arg);
		}
		bytebuf.append(stmt.consistency);

		fh.length = bytebuf.getIntLength;
		sock.write(fh.bytes);
		log("Sending: ", bytebuf.data);
		sock.write(bytebuf.data);
		log("-----=----=-");

		fh = readFrameHeader(sock, m_counter);
		throwOnError(fh);
		if (!fh.isRESULT) {
			throw new Exception("CQLProtocolException, Unknown response to Execute command: "~ to!string(fh.opcode));
		}
		return CassandraResult(lock, fh, sock, m_counter);
	}

	/**
	 *4.1.7. REGISTER
	 *
	 *  Register this connection to receive some type of events. The body of the
	 *  message is a [string list] representing the event types to register to. See
	 *  section 4.2.6 for the list of valid event types.
	 *
	 *  The response to a REGISTER message will be a READY message.
	 *
	 *  Please note that if a client driver maintains multiple connections to a
	 *  Cassandra node and/or connections to multiple nodes, it is advised to
	 *  dedicate a handful of connections to receive events, but to *not* register
	 *  for events on all connections, as this would only result in receiving
	 *  multiple times the same event messages, wasting bandwidth.
	 */
	void listen(Event[] events...) {
		connect();
		auto fh = makeHeader(FrameHeader.OpCode.register);
		auto bytebuf = appender!(ubyte[])();
		auto tmpbuf = appender!(ubyte[])();
		//tmpbuf.append(events); // FIXME: append(Event) isn't implemented
		fh.length = tmpbuf.getIntLength;
		bytebuf.put(fh.bytes);
		bytebuf.append(tmpbuf.data);
		sock.write(bytebuf.data);

		fh = readFrameHeader(sock, m_counter);
		if (!fh.isREADY) {
			throw new Exception("CQLProtocolException, Unknown response to REGISTER command");
		}
		assert(false, "Untested: setup of event listening");
	}

	/**
	 *4.2. Responses
	 *
	 *  This section describes the content of the frame body for the different
	 *  responses. Please note that to make room for future evolution, clients should
	 *  support extra informations (that they should simply discard) to the one
	 *  described in this document at the end of the frame body.
	 *
	 *4.2.1. ERROR
	 *
	 *  Indicates an error processing a request. The body of the message will be an
	 *  error code ([int]) followed by a [string] error message. Then, depending on
	 *  the exception, more content may follow. The error codes are defined in
	 *  Section 7, along with their additional content if any.
	 */
	protected void throwOnError(FrameHeader fh) {
		if (!fh.isERROR) return;
		int tmp;
		Error code;
		readIntNotNULL(tmp, sock, m_counter);
		code = cast(Error)tmp;

		auto msg = readShortString(sock, m_counter);

		auto spec_msg = toString(code);

		final switch (code) {
			case Error.serverError:
				throw new Exception("CQL Exception, "~ spec_msg ~ msg);
			case Error.protocolError:
				throw new Exception("CQL Exception, "~ spec_msg ~ msg);
			case Error.badCredentials:
				throw new Exception("CQL Exception, "~ spec_msg ~ msg);
			case Error.unavailableException:
				auto cs = cast(Consistency)readShort(sock, m_counter);
				auto required = readIntNotNULL(tmp, sock, m_counter);
				auto alive = readIntNotNULL(tmp, sock, m_counter);
				throw new Exception("CQL Exception, "~ spec_msg ~ msg ~" consistency:"~ .to!string(cs) ~" required:"~ to!string(required) ~" alive:"~ to!string(alive));
			case Error.overloaded:
				throw new Exception("CQL Exception, "~ spec_msg ~ msg);
			case Error.isBootstrapping:
				throw new Exception("CQL Exception, "~ spec_msg ~ msg);
			case Error.truncateError:
				throw new Exception("CQL Exception, "~ spec_msg ~ msg);
			case Error.writeTimeout:
				auto cl = cast(Consistency)readShort(sock, m_counter);
				auto received = readIntNotNULL(tmp, sock, m_counter);
				auto blockfor = readIntNotNULL(tmp, sock, m_counter); // WARN: the type for blockfor does not seem to be in the spec!!!
				auto writeType = cast(WriteType)readShortString(sock, m_counter);
				throw new Exception("CQL Exception, "~ spec_msg ~ msg ~" consistency:"~ to!string(cl) ~" received:"~ to!string(received) ~" blockfor:"~ to!string(blockfor) ~" writeType:"~ toString(writeType));
			case Error.readTimeout:
				auto cl = cast(Consistency)readShort(sock, m_counter);
				auto received = readIntNotNULL(tmp, sock, m_counter);
				auto blockfor = readIntNotNULL(tmp, sock, m_counter); // WARN: the type for blockfor does not seem to be in the spec!!!
				auto data_present = readByte(sock, m_counter);
				throw new Exception("CQL Exception, "~ spec_msg ~ msg ~" consistency:"~ to!string(cl) ~" received:"~ to!string(received) ~" blockfor:"~ to!string(blockfor) ~" data_present:"~ (data_present==0x00?"false":"true"));
			case Error.syntaxError:
				throw new Exception("CQL Exception, "~ spec_msg ~ msg);
			case Error.unauthorized:
				throw new Exception("CQL Exception, "~ spec_msg ~ msg);
			case Error.invalid:
				throw new Exception("CQL Exception, "~ spec_msg ~ msg);
			case Error.configError:
				throw new Exception("CQL Exception, "~ spec_msg ~ msg);
			case Error.alreadyExists:
				auto ks = readShortString(sock, m_counter);
				auto table = readShortString(sock, m_counter);
				throw new Exception("CQL Exception, "~ spec_msg ~ msg ~", keyspace:"~ks ~", table:"~ table );
			case Error.unprepared:
				auto unknown_id = readShort(sock, m_counter);
				throw new Exception("CQL Exception, "~ spec_msg ~ msg ~":"~ to!string(unknown_id));
		}
	}

	 /*
	 *4.2.2. READY
	 *
	 *  Indicates that the server is ready to process queries. This message will be
	 *  sent by the server either after a STARTUP message if no authentication is
	 *  required, or after a successful CREDENTIALS message.
	 *
	 *  The body of a READY message is empty.
	 *
	 *
	 *4.2.3. AUTHENTICATE
	 *
	 *  Indicates that the server require authentication. This will be sent following
	 *  a STARTUP message and must be answered by a CREDENTIALS message from the
	 *  client to provide authentication informations.
	 *
	 *  The body consists of a single [string] indicating the full class name of the
	 *  IAuthenticator in use.
	 */
	protected string readAuthenticate(FrameHeader fh) {
		assert(fh.isAUTHENTICATE);
		return readShortString(sock, m_counter);
	}

	/**
	 *4.2.4. SUPPORTED
	 *
	 *  Indicates which startup options are supported by the server. This message
	 *  comes as a response to an OPTIONS message.
	 *
	 *  The body of a SUPPORTED message is a [string multimap]. This multimap gives
	 *  for each of the supported STARTUP options, the list of supported values.
	 */
	protected StringMultiMap readSupported(FrameHeader fh) {
		return readStringMultiMap(sock, m_counter);
	}


	/**
	 *4.2.6. EVENT
	 *
	 *  And event pushed by the server. A client will only receive events for the
	 *  type it has REGISTER to. The body of an EVENT message will start by a
	 *  [string] representing the event type. The rest of the message depends on the
	 *  event type. The valid event types are:
	 *    - "TOPOLOGY_CHANGE": events related to change in the cluster topology.
	 *      Currently, events are sent when new nodes are added to the cluster, and
	 *      when nodes are removed. The body of the message (after the event type)
	 *      consists of a [string] and an [inet], corresponding respectively to the
	 *      type of change ("NEW_NODE" or "REMOVED_NODE") followed by the address of
	 *      the new/removed node.
	 *    - "STATUS_CHANGE": events related to change of node status. Currently,
	 *      up/down events are sent. The body of the message (after the event type)
	 *      consists of a [string] and an [inet], corresponding respectively to the
	 *      type of status change ("UP" or "DOWN") followed by the address of the
	 *      concerned node.
	 *    - "SCHEMA_CHANGE": events related to schema change. The body of the message
	 *      (after the event type) consists of 3 [string] corresponding respectively
	 *      to the type of schema change ("CREATED", "UPDATED" or "DROPPED"),
	 *      followed by the name of the affected keyspace and the name of the
	 *      affected table within that keyspace. For changes that affect a keyspace
	 *      directly, the table name will be empty (i.e. the empty string "").
	 *
	 *  All EVENT message have a streamId of -1 (Section 2.3).
	 *
	 *  Please note that "NEW_NODE" and "UP" events are sent based on internal Gossip
	 *  communication and as such may be sent a short delay before the binary
	 *  protocol server on the newly up node is fully started. Clients are thus
	 *  advise to wait a short time before trying to connect to the node (1 seconds
	 *  should be enough), otherwise they may experience a connection refusal at
	 *  first.
	 */
	enum Event : string {
		topologyChange = "TOPOLOGY_CHANGE",
		statusChange = "STATUS_CHANGE",
		schemaChange = "SCHEMA_CHANGE",
		newNode = "NEW_NODE",
		up = "UP"
	}
	protected void readEvent(FrameHeader fh) {
		assert(fh.isEVENT);
	}
	/*void writeEvents(Appender!(ubyte[]) appender, Event e...) {
		appender.append(e);
	}*/


	/**5. Compression
	 *
	 *  Frame compression is supported by the protocol, but then only the frame body
	 *  is compressed (the frame header should never be compressed).
	 *
	 *  Before being used, client and server must agree on a compression algorithm to
	 *  use, which is done in the STARTUP message. As a consequence, a STARTUP message
	 *  must never be compressed.  However, once the STARTUP frame has been received
	 *  by the server can be compressed (including the response to the STARTUP
	 *  request). Frame do not have to be compressed however, even if compression has
	 *  been agreed upon (a server may only compress frame above a certain size at its
	 *  discretion). A frame body should be compressed if and only if the compressed
	 *  flag (see Section 2.2) is set.
	 */

	/**
	 *6. Collection types
	 *
	 *  This section describe the serialization format for the collection types:
	 *  list, map and set. This serialization format is both useful to decode values
	 *  returned in RESULT messages but also to encode values for EXECUTE ones.
	 *
	 *  The serialization formats are:
	 *     List: a [short] n indicating the size of the list, followed by n elements.
	 *           Each element is [short bytes] representing the serialized element
	 *           value.
	 */
	protected auto readList(T)(FrameHeader fh) {
		auto size = readShort(fh);
		T[] ret;
		foreach (i; 0..size) {
			ret ~= readBytes!T(sock, m_counter);
		}
		return ret;

	}

	/**     Map: a [short] n indicating the size of the map, followed by n entries.
	 *          Each entry is composed of two [short bytes] representing the key and
	 *          the value of the entry map.
	 */
	protected auto readMap(T,U)(FrameHeader fh) {
		auto size = readShort(fh);
		T[U] ret;
		foreach (i; 0..size) {
			ret[readShortBytes!T(sock, m_counter)] = readShortBytes!U(sock, m_counter);

		}
		return ret;
	}

	/**     Set: a [short] n indicating the size of the set, followed by n elements.
	 *          Each element is [short bytes] representing the serialized element
	 *          value.
	 */
	protected auto readSet(T)(FrameHeader fh) {
		auto size = readShort(fh);
		T[] ret;
		foreach (i; 0..size) {
			ret[] ~= readBytes!T(sock, m_counter);

		}
		return ret;
	}

	/**
	 *7. Error codes
	 *
	 *  The supported error codes are described below:
	 *    0x0000    Server error: something unexpected happened. This indicates a
	 *              server-side bug.
	 *    0x000A    Protocol error: some client message triggered a protocol
	 *              violation (for instance a QUERY message is sent before a STARTUP
	 *              one has been sent)
	 *    0x0100    Bad credentials: CREDENTIALS request failed because Cassandra
	 *              did not accept the provided credentials.
	 *
	 *    0x1000    Unavailable exception. The rest of the ERROR message body will be
	 *                <cl><required><alive>
	 *              where:
	 *                <cl> is the [consistency] level of the query having triggered
	 *                     the exception.
	 *                <required> is an [int] representing the number of node that
	 *                           should be alive to respect <cl>
	 *                <alive> is an [int] representing the number of replica that
	 *                        were known to be alive when the request has been
	 *                        processed (since an unavailable exception has been
	 *                        triggered, there will be <alive> < <required>)
	 *    0x1001    Overloaded: the request cannot be processed because the
	 *              coordinator node is overloaded
	 *    0x1002    Is_bootstrapping: the request was a read request but the
	 *              coordinator node is bootstrapping
	 *    0x1003    Truncate_error: error during a truncation error.
	 *    0x1100    Write_timeout: Timeout exception during a write request. The rest
	 *              of the ERROR message body will be
	 *                <cl><received><blockfor><writeType>
	 *              where:
	 *                <cl> is the [consistency] level of the query having triggered
	 *                     the exception.
	 *                <received> is an [int] representing the number of nodes having
	 *                           acknowledged the request.
	 *                <blockfor> is the number of replica whose acknowledgement is
	 *                           required to achieve <cl>.
	 *                <writeType> is a [string] that describe the type of the write
	 *                            that timeouted. The value of that string can be one
	 *                            of:
	 *                             - "SIMPLE": the write was a non-batched
	 *                               non-counter write.
	 *                             - "BATCH": the write was a (logged) batch write.
	 *                               If this type is received, it means the batch log
	 *                               has been successfully written (otherwise a
	 *                               "BATCH_LOG" type would have been send instead).
	 *                             - "UNLOGGED_BATCH": the write was an unlogged
	 *                               batch. Not batch log write has been attempted.
	 *                             - "COUNTER": the write was a counter write
	 *                               (batched or not).
	 *                             - "BATCH_LOG": the timeout occured during the
	 *                               write to the batch log when a (logged) batch
	 *                               write was requested.
	 */
	 alias string WriteType;
	 string toString(WriteType wt) {
		final switch (cast(string)wt) {
			case "SIMPLE":
				return "SIMPLE: the write was a non-batched non-counter write.";
			case "BATCH":
				return "BATCH: the write was a (logged) batch write. If this type is received, it means the batch log has been successfully written (otherwise a \"BATCH_LOG\" type would have been send instead).";
			case "UNLOGGED_BATCH":
				return "UNLOGGED_BATCH: the write was an unlogged batch. Not batch log write has been attempted.";
			case "COUNTER":
				return "COUNTER: the write was a counter write (batched or not).";
			case "BATCH_LOG":
				return "BATCH_LOG: the timeout occured during the write to the batch log when a (logged) batch write was requested.";
		 }
	 }
	 /**    0x1200    Read_timeout: Timeout exception during a read request. The rest
	 *              of the ERROR message body will be
	 *                <cl><received><blockfor><data_present>
	 *              where:
	 *                <cl> is the [consistency] level of the query having triggered
	 *                     the exception.
	 *                <received> is an [int] representing the number of nodes having
	 *                           answered the request.
	 *                <blockfor> is the number of replica whose response is
	 *                           required to achieve <cl>. Please note that it is
	 *                           possible to have <received> >= <blockfor> if
	 *                           <data_present> is false. And also in the (unlikely)
	 *                           case were <cl> is achieved but the coordinator node
	 *                           timeout while waiting for read-repair
	 *                           acknowledgement.
	 *                <data_present> is a single byte. If its value is 0, it means
	 *                               the replica that was asked for data has not
	 *                               responded. Otherwise, the value is != 0.
	 *
	 *    0x2000    Syntax_error: The submitted query has a syntax error.
	 *    0x2100    Unauthorized: The logged user doesn't have the right to perform
	 *              the query.
	 *    0x2200    Invalid: The query is syntactically correct but invalid.
	 *    0x2300    Config_error: The query is invalid because of some configuration issue
	 *    0x2400    Already_exists: The query attempted to create a keyspace or a
	 *              table that was already existing. The rest of the ERROR message
	 *              body will be <ks><table> where:
	 *                <ks> is a [string] representing either the keyspace that
	 *                     already exists, or the keyspace in which the table that
	 *                     already exists is.
	 *                <table> is a [string] representing the name of the table that
	 *                        already exists. If the query was attempting to create a
	 *                        keyspace, <table> will be present but will be the empty
	 *                        string.
	 *    0x2500    Unprepared: Can be thrown while a prepared statement tries to be
	 *              executed if the provide prepared statement ID is not known by
	 *              this host. The rest of the ERROR message body will be [short
	 *              bytes] representing the unknown ID.
	 **/
	 enum Error : ushort {
		serverError = 0x0000,
		protocolError = 0x000A,
		badCredentials = 0x0100,
		unavailableException = 0x1000,
		overloaded = 0x1001,
		isBootstrapping = 0x1002,
		truncateError = 0x1003,
		writeTimeout = 0x1100,
		readTimeout = 0x1200,
		syntaxError = 0x2000,
		unauthorized = 0x2100,
		invalid = 0x2200,
		configError = 0x2300,
		alreadyExists = 0x2400,
		unprepared = 0x2500
	 }

	 static string toString(Error err)
	 {
		switch (err) {
			case Error.serverError:
				return "Server error: something unexpected happened. This indicates a server-side bug.";
			case Error.protocolError:
				return "Protocol error: some client message triggered a protocol violation (for instance a QUERY message is sent before a STARTUP one has been sent)";
			case Error.badCredentials:
				return "Bad credentials: CREDENTIALS request failed because Cassandra did not accept the provided credentials.";
			case Error.unavailableException:
				return "Unavailable exception.";
			case Error.overloaded:
				return "Overloaded: the request cannot be processed because the coordinator node is overloaded";
			case Error.isBootstrapping:
				return "Is_bootstrapping: the request was a read request but the coordinator node is bootstrapping";
			case Error.truncateError:
				return "Truncate_error: error during a truncation error.";
			case Error.writeTimeout:
				return "Write_timeout: Timeout exception during a write request.";
			case Error.readTimeout:
				return "Read_timeout: Timeout exception during a read request.";
			case Error.syntaxError:
				return "Syntax_error: The submitted query has a syntax error.";
			case Error.unauthorized:
				return "Unauthorized: The logged user doesn't have the right to perform the query.";
			case Error.invalid:
				return "Invalid: The query is syntactically correct but invalid.";
			case Error.configError:
				return "Config_error: The query is invalid because of some configuration issue.";
			case Error.alreadyExists:
				return "Already_exists: The query attempted to create a keyspace or a table that was already existing.";
			case Error.unprepared:
				return "Unprepared: Can be thrown while a prepared statement tries to be executed if the provide prepared statement ID is not known by this host.";
			default:
				assert(false);
		}
	 }
}
