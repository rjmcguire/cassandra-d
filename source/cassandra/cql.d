module cassandra.cql;

import std.bitmanip : bitfields;
import std.conv;
import std.traits;

import std.format : formattedWrite;
import std.array;
import std.stdint;

import std.stdio : writeln, writef, writefln;

import utils;

version (Have_vibe_d) {
	pragma(msg, "build cassandra-d with vibe");
	import vibe.core.net : TcpConnection, connectTcp;
} else {
	pragma(msg, "build cassandra-d no vibe");
	import tcpconnection;
}

import serialize;

class Connection {
	TcpConnection sock;
	this(){}

	enum defaultport = 9042;
	void connect(string host, short port = defaultport) {
		writeln("connecting");
		sock = connectTcp(host, port);
		writeln("connected. doing handshake...");
		startup();
		writeln("handshake completed.");
	}

	void close() {
		if (counter > 0) {
			auto buf = readRawBytes(sock, counter, counter);
			writeln("buf:", buf);
		}
		assert(counter == 0, "Did not complete reading of stream: "~ to!string(counter) ~" bytes left");
		sock.close();
	}



	private void write(TcpConnection s, Appender!(ubyte[]) appender) {
		//print(appender.data);
		s.write(appender.data, true);
	}


	//vvvvvvvvvvvvvvvvvvvvv CQL Implementation vvvvvvvvvvvvvvvvvv
	int counter; // keeps track of data left after each read

	bool compression_enabled_;
	bool tracing_enabled_;

	byte streamid_;
	FrameHeader.Version transport_version_; // this is just the protocol version number
	/// Make a FrameHeader corresponding to this Stream
	FrameHeader makeHeader(FrameHeader.OpCode opcode) {
		FrameHeader fh;
		version (CassandraV2) {
			fh.version_ = FrameHeader.Version.V2Request;
		} else {
			fh.version_ = FrameHeader.Version.V1Request;
		}
		if (compression_enabled_)
			fh.compress = true;
		if (tracing_enabled_)
			fh.trace = true;
		fh.streamid = streamid_;
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
	 void startup(string compression_algorithm = "") {
		StringMap data;
		data["CQL_VERSION"] = "3.0.0";
		if (compression_algorithm.length > 0)
			data["COMPRESSION"] = compression_algorithm;

		auto fh = makeHeader(FrameHeader.OpCode.STARTUP);

		auto bytebuf = appender!(ubyte[])();
		bytebuf.append(data);
		fh.length = bytebuf.getIntLength();
		write(sock, appender!(ubyte[])().append(fh.bytes));
		write(sock, bytebuf);

		fh = readFrameHeader(sock, counter);
		if (fh.isAUTHENTICATE) {
			fh = authenticate(fh);
		}
		throwOnError(fh);
	}
	FrameHeader authenticate(FrameHeader fh) {
		auto authenticatorname = readAuthenticate(fh);
		auto authenticator = GetAuthenticator(authenticatorname);
		version (CassandraV2) {
			sendAuthResponse(authenticator);
		} else {
			sendCredentials(authenticator.getCredentials());
		}
		throw new Exception("NotImplementedException Authentication: "~ authenticatorname);
	}

	version (CassandraV2) {
		/**
		 * 4.1.2. AUTH_RESPONSE
		 *
		 * Answers a server authentication challenge.
		 *
		 * Authentication in the protocol is SASL based. The server sends authentication
		 * challenges (a bytes token) to which the client answer with this message. Those
		 * exchanges continue until the server accepts the authentication by sending a
		 * AUTH_SUCCESS message after a client AUTH_RESPONSE. It is however that client that
		 * initiate the exchange by sending an initial AUTH_RESPONSE in response to a
		 * server AUTHENTICATE request.
		 *
		 * The body of this message is a single [bytes] token. The details of what this
		 * token contains (and when it can be null/empty, if ever) depends on the actual
		 * authenticator used.
		 *
		 * The response to a AUTH_RESPONSE is either a follow-up AUTH_CHALLENGE message,
		 * an AUTH_SUCCESS message or an ERROR message.
		 */
		void sendAuthResponse(Authenticator a) {
			assert(false, "todo, not implemented sendAuthResponse");
			throw new Exception("NotImplementedException");
		}
	} else {
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
		void sendCredentials(StringMap data) {
			auto fh = makeHeader(FrameHeader.OpCode.CREDENTIALS);
			auto bytebuf = appender!(ubyte[])();
			bytebuf.append(data);
			fh.length = bytebuf.getIntLength;
			write(sock, appender!(ubyte[])().append(fh.bytes));
			write(sock, bytebuf);

			assert(false, "todo: read credentials response");
		}
	}


	/**
	 *4.1.3. OPTIONS
	 *
	 *  Asks the server to return what STARTUP options are supported. The body of an
	 *  OPTIONS message should be empty and the server will respond with a SUPPORTED
	 *  message.
	 */
	StringMultiMap requestOptions() {
		auto fh = makeHeader(FrameHeader.OpCode.OPTIONS);
		write(sock, appender!(ubyte[])().append(fh.bytes));
		fh = readFrameHeader(sock, counter);
		if (!fh.isSUPPORTED) {
			throw new Exception("CQLProtocolException, Unknown response to OPTIONS request");
		}
		return readSupported(fh);
	}

	 /**
	 *4.1.4. QUERY
	 *
	 *	V1:  Performs a CQL query. The body of the message consists of a CQL query as a [long
	 *  	 string] followed by the [consistency] for the operation.
	 *
	 *  V2:	 Performs a CQL query. The body of the message must be:
     *		 	<query><query_parameters>
  	 *		 where <query> is a [long string] representing the query and
  	 *		 	<query_parameters> must be
     *			<consistency><flags>[<n><value_1>...<value_n>][<result_page_size>][<paging_state>][<serial_consistency>]
  	 *		 where:
     *		 	- <consistency> is the [consistency] level for the operation.
     *		 	- <flags> is a [byte] whose bits define the options for this query and
     * 			in particular influence what the remainder of the message contains.
     * 			A flag is set if the bit corresponding to its `mask` is set. Supported
	 *	        flags are, given there mask:
	 *	        	0x01: Values. In that case, a [short] <n> followed by <n> [bytes]
	 *	            	  values are provided. Those value are used for bound variables in
	 *		              the query.
	 *		        0x02: Skip_metadata. If present, the Result Set returned as a response
	 *		              to that query (if any) will have the NO_METADATA flag (see
	 *	    	          Section 4.2.5.2).
	 *	        	0x03: Page_size. In that case, <result_page_size> is an [int]
	 *	            	  controlling the desired page size of the result (in CQL3 rows).
	 *	              	See the section on paging (Section 7) for more details.
	 *	        	0x04: With_paging_state. If present, <paging_state> should be present.
	 *	            	<paging_state> is a [bytes] value that should have been returned
	 *	              	in a result set (Section 4.2.5.2). If provided, the query will be
	 *	              	executed but starting from a given paging state. This also to
	 *	              	continue paging on a different node from the one it has been
	 *	              	started (See Section 7 for more details).
	 *	        	0x05: With serial consistency. If present, <serial_consistency> should be
	 *	            	  present. <serial_consistency> is the [consistency] level for the
	 *	              	serial phase of conditional updates. That consitency can only be
	 *	              	either SERIAL or LOCAL_SERIAL and if not present, it defaults to
	 *	              	SERIAL. This option will be ignored for anything else that a
	 *	              	conditional update/insert.

	WARNING THIS PART OF THE DOCUMENTATION IS NOT ACCURATE!!! THE flag's value is actually its java enum ordinal see:
	java.org.apache.cassandra.cql3.QueryOptions.java : Flag.deserialize and Flag.serialize
	 */
	private enum QueryFlag : ubyte {
		Values 					= 0x01,
		Skip_metadata			= 0x02,
		Page_size				= 0x04,
		With_paging_state		= 0x08,
		With_serial_consistency	= 0x10
	}
	/**
	 *  Note that the consistency is ignored by some queries (USE, CREATE, ALTER,
	 *  TRUNCATE, ...).
	 *
	 *  The server will respond to a QUERY message with a RESULT message, the content
	 *  of which depends on the query.
	 */
	version (CassandraV2) {
		auto appendQueryParameters(Args...)(ref Appender!(ubyte[]) bytebuf, Consistency consistency, ubyte flags, int page_size, byte[] paging_state, Consistency serial_consistency, Args args) {
			bytebuf.append(consistency);
			bytebuf.append(flags);
			if ((flags & QueryFlag.Values) == QueryFlag.Values) {
				assert(args.length < short.max);
				bytebuf.append(cast(short)args.length);
				foreach (arg; args) {
					bytebuf.appendIntBytes(arg);
				}
			}
			if ((flags & QueryFlag.Page_size) == QueryFlag.Page_size) {
				bytebuf.append(page_size);
			}
			if ((flags & QueryFlag.With_paging_state) == QueryFlag.With_paging_state) {
				bytebuf.appendIntBytes(paging_state);
			}
			if ((flags & QueryFlag.With_serial_consistency) == QueryFlag.With_serial_consistency) {
				bytebuf.append(serial_consistency);
			}
			return bytebuf;
		}
		Result query(Args...)(string q, Consistency consistency) {
			return query(q, consistency, 0x00, -1, null, Consistency.SERIAL);
		}
		Result query(Args...)(string q, Consistency consistency, ubyte flags, int page_size, byte[] paging_state, Consistency serial_consistency, Args args) {
			assert(serial_consistency == Consistency.SERIAL || serial_consistency == Consistency.LOCAL_SERIAL);

			auto fh = makeHeader(FrameHeader.OpCode.QUERY);
			auto bytebuf = appender!(ubyte[])();
			writeln("-----------");
			bytebuf.appendLongString(q);
			//print(bytebuf.data);
			writeln("-----------");
			bytebuf = appendQueryParameters(bytebuf, consistency, flags, page_size, paging_state, serial_consistency, args);

			fh.length = bytebuf.getIntLength;
			write(sock, appender!(ubyte[])().append(fh.bytes));
			write(sock, bytebuf);

			fh = readFrameHeader(sock, counter);
			throwOnError(fh);
			return new Result(fh);
		}
	} else {
		Result query(string q, Consistency consistency) {
			auto fh = makeHeader(FrameHeader.OpCode.QUERY);
			auto bytebuf = appender!(ubyte[])();
			writeln("-----------");
			bytebuf.appendLongString(q);
			//print(bytebuf.data);
			writeln("-----------");
			bytebuf.append(consistency);
			fh.length = bytebuf.getIntLength;
			write(sock, appender!(ubyte[])().append(fh.bytes));
			write(sock, bytebuf);

			fh = readFrameHeader(sock, counter);
			throwOnError(fh);
			return new Result(fh);
		}
	}


	bool insert(string q, Consistency consistency = Consistency.ANY) {
		assert(q[0.."insert".length]=="INSERT");
		auto res = query(q, consistency);
		if (res.kind == Result.Kind.Void) {
			return true;
		}
		throw new Exception("CQLProtocolException: expected void response to insert");
	}
	Result select(string q, Consistency consistency = Consistency.QUORUM) {
		import std.string : icmp;
		assert(icmp(q[0.."select".length], "SELECT")==0);
		return query(q, consistency);
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
		auto fh = makeHeader(FrameHeader.OpCode.PREPARE);
		auto bytebuf = appender!(ubyte[])();
		writeln("---------=-");
		bytebuf.appendLongString(q);
		fh.length = bytebuf.getIntLength;
		write(sock, appender!(ubyte[])().append(fh.bytes));
		write(sock, bytebuf);
		writeln("---------=-");



		fh = readFrameHeader(sock, counter);
		throwOnError(fh);
		if (!fh.isRESULT) {
			throw new Exception("CQLProtocolException, Unknown response to PREPARE command");
		}
		return new PreparedStatement(fh);
	}

	/**
	 *4.1.6. EXECUTE
	 *
	 *  Executes a prepared query. The body of the message must be:
	 *  V1:   <id><n><value_1>....<value_n><consistency>
	 *  		where:
	 *  		  - <id> is the prepared query ID. It's the [short bytes] returned as a
	 *  		    response to a PREPARE message.
	 *  		  - <n> is a [short] indicating the number of following values.
	 *  		  - <value_1>...<value_n> are the [bytes] to use for bound variables in the
	 *  		    prepared query.
	 *  		  - <consistency> is the [consistency] level for the operation.
	 *
	 *  	Note that the consistency is ignored by some (prepared) queries (USE, CREATE,
	 *  	ALTER, TRUNCATE, ...).
	 *
	 *  V2:   <id><query_parameters>
	 *			where <id> is the prepared query ID. It's the [short bytes] returned as a
	 *			response to a PREPARE message. As for <query_parameters>, it has the exact
	 *			same definition than in QUERY (see Section 4.1.4).
	 *
	 *
	 *  The response from the server will be a RESULT message.
	 */
	version (CassandraV2) {
		private auto execute(Args...)(ubyte[] preparedStatementID, Consistency consistency, ubyte flags, int page_size, byte[] paging_state, Consistency serial_consistency, Args args) {
			auto fh = makeHeader(FrameHeader.OpCode.EXECUTE);
			auto bytebuf = appender!(ubyte[])();
			writeln("-----=----=-");
			bytebuf.appendShortBytes(preparedStatementID);

			bytebuf = appendQueryParameters(bytebuf, consistency, flags, page_size, paging_state, serial_consistency, args);

			fh.length = bytebuf.getIntLength;
			write(sock, appender!(ubyte[])().append(fh.bytes));
			writeln("Sending: ", bytebuf.data);
			write(sock, bytebuf);
			writeln("-----=----=-");

			fh = readFrameHeader(sock, counter);
			throwOnError(fh);
			if (!fh.isRESULT) {
				throw new Exception("CQLProtocolException, Unknown response to Execute command: "~ to!string(fh.opcode));
			}
			return new Result(fh);
		}
	} else {
		private auto execute(Args...)(ubyte[] preparedStatementID, Consistency consistency, Args args) {
			auto fh = makeHeader(FrameHeader.OpCode.EXECUTE);
			auto bytebuf = appender!(ubyte[])();
			writeln("-----=----=-");
			bytebuf.appendShortBytes(preparedStatementID);
			assert(args.length < short.max);
			bytebuf.append(cast(short)args.length);
			foreach (arg; args) {
				bytebuf.appendIntBytes(arg);
			}
			bytebuf.append(consistency);

			fh.length = bytebuf.getIntLength;
			write(sock, appender!(ubyte[])().append(fh.bytes));
			writeln("Sending: ", bytebuf.data);
			write(sock, bytebuf);
			writeln("-----=----=-");

			fh = readFrameHeader(sock, counter);
			throwOnError(fh);
			if (!fh.isRESULT) {
				throw new Exception("CQLProtocolException, Unknown response to Execute command: "~ to!string(fh.opcode));
			}
			return new Result(fh);
		}
	}

	version (CassandraV2) {
		/**
		 * 4.1.7. BATCH
		 *	  Allows executing a list of queries (prepared or not) as a batch (note that
		 *	  only DML statements are accepted in a batch). The body of the message must
		 *	  be:
		 *	    <type><n><query_1>...<query_n><consistency>
		 *	  where:
		 *	    - <type> is a [byte] indicating the type of batch to use:
		 *	        - If <type> == 0, the batch will be "logged". This is equivalent to a
		 *	          normal CQL3 batch statement.
		 *	        - If <type> == 1, the batch will be "unlogged".
		 *	        - If <type> == 2, the batch will be a "counter" batch (and non-counter
		 *	          statements will be rejected).
		 *	    - <n> is a [short] indicating the number of following queries.
		 *	    - <query_1>...<query_n> are the queries to execute. A <query_i> must be of the
		 *	      form:
		 *	        <kind><string_or_id><n><value_1>...<value_n>
		 *	      where:
		 *	       - <kind> is a [byte] indicating whether the following query is a prepared
		 *	         one or not. <kind> value must be either 0 or 1.
		 *	       - <string_or_id> depends on the value of <kind>. If <kind> == 0, it should be
		 *	         a [long string] query string (as in QUERY, the query string might contain
		 *	         bind markers). Otherwise (that is, if <kind> == 1), it should be a
		 *	         [short bytes] representing a prepared query ID.
		 *	       - <n> is a [short] indicating the number (possibly 0) of following values.
		 *	       - <value_1>...<value_n> are the [bytes] to use for bound variables.
		 *	    - <consistency> is the [consistency] level for the operation.
		 *
		 *	  The server will respond with a RESULT message with a `Void` kind (0x0001,
		 *	  see Section 4.2.5).
		 */
		 Result batch() {
		 	throw new NotImplementedException("BATCH not implemented");
		 }
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
	void listen(Event events...) {
		auto fh = makeHeader(FrameHeader.OpCode.REGISTER);
		auto bytebuf = appender!(ubyte[])();
		auto tmpbuf = appender!(ubyte[])();
		tmpbuf.append(events);
		fh.length = tmpbuf.getIntLength;
		bytebuf.put(fh.bytes);
		bytebuf.append(tmpbuf);
		write(sock, bytebuf);

		fh = readFrameHeader(sock, counter);
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
	void throwOnError(FrameHeader fh) {
		if (!fh.isERROR) return;
		int tmp;
		Error code;
		readIntNotNULL(tmp, sock, counter);
		code = cast(Error)tmp;

		auto msg = readShortString(sock,counter);

		auto spec_msg = toString(code);

		final switch (code) {
			case Error.ServerError:
				throw new Exception("CQL Exception, "~ spec_msg ~ msg);
			case Error.ProtocolError:
				throw new Exception("CQL Exception, "~ spec_msg ~ msg);
			case Error.BadCredentials:
				throw new Exception("CQL Exception, "~ spec_msg ~ msg);
			case Error.UnavailableException:
				auto cs = cast(Consistency)readShort(sock,counter);
				auto required = readIntNotNULL(tmp, sock, counter);
				auto alive = readIntNotNULL(tmp, sock, counter);
				throw new Exception("CQL Exception, "~ spec_msg ~ msg ~" consistency:"~ .to!string(cs) ~" required:"~ to!string(required) ~" alive:"~ to!string(alive));
			case Error.Overloaded:
				throw new Exception("CQL Exception, "~ spec_msg ~ msg);
			case Error.IsBootstrapping:
				throw new Exception("CQL Exception, "~ spec_msg ~ msg);
			case Error.TruncateError:
				throw new Exception("CQL Exception, "~ spec_msg ~ msg);
			case Error.WriteTimeout:
				auto cl = cast(Consistency)readShort(sock,counter);
				auto received = readIntNotNULL(tmp, sock, counter);
				auto blockfor = readIntNotNULL(tmp, sock, counter); // WARN: the type for blockfor does not seem to be in the spec!!!
				auto writeType = cast(WriteType)readShortString(sock, counter);
				throw new Exception("CQL Exception, "~ spec_msg ~ msg ~" consistency:"~ to!string(cl) ~" received:"~ to!string(received) ~" blockfor:"~ to!string(blockfor) ~" writeType:"~ toString(writeType));
			case Error.ReadTimeout:
				auto cl = cast(Consistency)readShort(sock,counter);
				auto received = readIntNotNULL(tmp, sock, counter);
				auto blockfor = readIntNotNULL(tmp, sock, counter); // WARN: the type for blockfor does not seem to be in the spec!!!
				auto data_present = readByte(sock, counter);
				throw new Exception("CQL Exception, "~ spec_msg ~ msg ~" consistency:"~ to!string(cl) ~" received:"~ to!string(received) ~" blockfor:"~ to!string(blockfor) ~" data_present:"~ (data_present==0x00?"false":"true"));
			case Error.SyntaxError:
				throw new Exception("CQL Exception, "~ spec_msg ~ msg);
			case Error.Unauthorized:
				throw new Exception("CQL Exception, "~ spec_msg ~ msg);
			case Error.Invalid:
				throw new Exception("CQL Exception, "~ spec_msg ~ msg);
			case Error.ConfigError:
				throw new Exception("CQL Exception, "~ spec_msg ~ msg);
			case Error.AlreadyExists:
				auto ks = readShortString(sock,counter);
				auto table = readShortString(sock,counter);
				throw new Exception("CQL Exception, "~ spec_msg ~ msg ~", keyspace:"~ks ~", table:"~ table );
			case Error.Unprepared:
				auto unknown_id = readShort(sock,counter);
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
	 *  V1: Indicates that the server require authentication. This will be sent following
	 *  	a STARTUP message and must be answered by a CREDENTIALS message from the
	 *  	client to provide authentication informations.
	 *  V2: Indicates that the server require authentication, and which authentication
	 *		mechanism to use.
	 *
	 *		The authentication is SASL based and thus consists on a number of server
	 *		challenges (AUTH_CHALLENGE, Section 4.2.7) followed by client responses
	 *		(AUTH_RESPONSE, Section 4.1.2). The Initial exchange is however boostrapped
	 *		by an initial client response. The details of that exchange (including how
	 *		much challenge-response pair are required) are specific to the authenticator
	 *		in use. The exchange ends when the server sends an AUTH_SUCCESS message or
	 *		an ERROR message.
	 *
	 *		This message will be sent following a STARTUP message if authentication is
	 *		required and must be answered by a AUTH_RESPONSE message from the client.
	 *
	 *  The body consists of a single [string] indicating the full class name of the
	 *  IAuthenticator in use.
	 */
	string readAuthenticate(FrameHeader fh) {
		assert(fh.isAUTHENTICATE);
		return readShortString(sock, counter);
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
	StringMultiMap readSupported(FrameHeader fh) {
		return readStringMultiMap(sock, counter);
	}

	/**
	 *4.2.5. RESULT
	 *
	 *  V1: The result to a query (QUERY, PREPARE or EXECUTE messages).
	 *
	 *  V2: The result to a query (QUERY, PREPARE, EXECUTE or BATCH messages).
	 *
	 * The first element of the body of a RESULT message is an [int] representing the
	 *  `kind` of result. The rest of the body depends on the kind. The kind can be
	 * one of:
	 *    0x0001    Void: for results carrying no information.
	 *    0x0002    Rows: for results to select queries, returning a set of rows.
	 *    0x0003    Set_keyspace: the result to a `use` query.
	 *    0x0004    Prepared: result to a PREPARE message.
	 *    0x0005    Schema_change: the result to a schema altering query.
	 *
	 *  The body for each kind (after the [int] kind) is defined below.
	 */
	class Result {
		FrameHeader fh;
		private Kind kind_;
		Kind kind() { return kind_; }

		this(FrameHeader fh) {
			this.fh = fh;
			int tmp;
			kind_ = cast(Kind)readIntNotNULL(tmp, sock, counter);
			final switch (kind_) {
				case Kind.Void:
					readVoid(fh); break;
				case Kind.Rows:
					readRows(fh); break;
				case Kind.Set_keyspace:
					readSet_keyspace(fh); break;
				case Kind.Prepared:
					readPrepared(fh); break;
				case Kind.Schema_change:
					readSchema_change(fh); break;
			}
		}

		auto rows() {
			assert(kind_ == Kind.Rows);
			return rows_;
		}

		enum Kind : short {
			Void = 0x0001,
			Rows = 0x0002,
			Set_keyspace = 0x0003,
			Prepared = 0x0004,
			Schema_change = 0x0005
		};
		/**
		 *4.2.5.1. Void
		 *
		 *  The rest of the body for a Void result is empty. It indicates that a query was
		 *  successful without providing more information.
		 */
		void readVoid(FrameHeader fh) {
			assert(kind_ == Kind.Void);
		}
		/**
		 *4.2.5.2. Rows
		 *
		 *  Indicates a set of rows. The rest of body of a Rows result is:
		 *    <metadata><rows_count><rows_content>
		 */
		MetaData metadata;
		ubyte[][][] rows_;
		auto readRows(FrameHeader fh) {
			assert(kind_ == Kind.Rows || kind_ == Kind.Prepared);
			metadata = readRowMetaData(fh);
			// <rows_count> is read within readRowsContent
			rows_ = readRowsContent(fh, metadata);
		}

		/**  where:
		 *    - <metadata> is composed of:
		 *      V1:  <flags><columns_count><global_table_spec>?<col_spec_1>...<col_spec_n>
		 *		V2:  <flags><columns_count>[<paging_state>][<global_table_spec>?<col_spec_1>...<col_spec_n>]
		 *      where:
		 *        - <flags> is an [int]. The bits of <flags> provides information on the
		 *          formatting of the remaining informations. A flag is set if the bit
		 *          corresponding to its `mask` is set. Supported flags are, given there
		 *          mask:
		 *            0x0001    Global_tables_spec: if set, only one table spec (keyspace
		 *                      and table name) is provided as <global_table_spec>. If not
		 *                      set, <global_table_spec> is not present.
		 *			V2:
		 *				0x0002    Has_more_pages: indicates whether this is not the last
		 *				          page of results and more should be retrieve. If set, the
		 *				          <paging_state> will be present. The <paging_state> is a
		 *				          [bytes] value that should be used in QUERY/EXECUTE to
		 *				          continue paging and retrieve the remained of the result for
		 *				          this query (See Section 7 for more details).
		 *				0x0003    No_metadata: if set, the <metadata> is only composed of
		 *				          these <flags>, the <column_count> and optionally the
		 *				          <paging_state> (depending on the Has_more_pages flage) but
		 *				          no other information (so no <global_table_spec> nor <col_spec_i>).
		 *				          This will only ever be the case if this was requested
		 *				          during the query (see QUERY and RESULT messages).
		 *        - <columns_count> is an [int] representing the number of columns selected
		 *          by the query this result is of. It defines the number of <col_spec_i>
		 *          elements in and the number of element for each row in <rows_content>.
		 *        - <global_table_spec> is present if the Global_tables_spec is set in
		 *          <flags>. If present, it is composed of two [string] representing the
		 *          (unique) keyspace name and table name the columns return are of.
		 */
		struct MetaData {
			int flags;
			enum {
				GLOBAL_TABLES_SPEC = 0x0001,
				Has_more_pages = 0x0002,
				No_metadata = 0x0004
			}
			@property bool hasGlobalTablesSpec() { return flags & MetaData.GLOBAL_TABLES_SPEC ? true : false; }
			int columns_count;
			ubyte[] paging_state;
			string[2] global_table_spec;
			ColumnSpecification[] column_specs;
		}
		MetaData readRowMetaData(FrameHeader fh) {
			auto md = MetaData();
			md.flags.readIntNotNULL(sock, counter);
			md.columns_count.readIntNotNULL(sock, counter);
			if ((md.flags & MetaData.Has_more_pages) == MetaData.Has_more_pages) {
				md.paging_state = readIntBytes(sock, counter);
			}
			if ((md.flags & MetaData.No_metadata) != MetaData.No_metadata) {
				if (md.flags & MetaData.GLOBAL_TABLES_SPEC) {
					md.global_table_spec[0] = readShortString(sock, counter);
					md.global_table_spec[1] = readShortString(sock, counter);
				}
				md.column_specs = readColumnSpecifications(md.flags & MetaData.GLOBAL_TABLES_SPEC, md.columns_count);
			}
			writeln("got MetaData: ", md);
			return md;
		}

		/**       - <col_spec_i> specifies the columns returned in the query. There is
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
		 *            0x000A    Text // Gone in V2
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
		Option* readOption(TcpConnection s, ref int counter) {
			auto ret = new Option();
			ret.id = cast(Option.Type)readShort(s, counter);
			final switch (ret.id) {
				case Option.Type.Custom:
					ret.string_value = readShortString(s, counter);
					break;
				case Option.Type.Ascii:
					break;
				case Option.Type.Bigint:
					break;
				case Option.Type.Blob:
					break;
				case Option.Type.Boolean:
					break;
				case Option.Type.Counter:
					break;
				case Option.Type.Decimal:
					break;
				case Option.Type.Double:
					break;
				case Option.Type.Float:
					break;
				case Option.Type.Int:
					break;
				version (CassandraV2) {} else {
				case Option.Type.Text:
					break;
				}
				case Option.Type.Timestamp:
					break;
				case Option.Type.Uuid:
					break;
				case Option.Type.Varchar:
					break;
				case Option.Type.Varint:
					break;
				case Option.Type.Timeuuid:
					break;
				case Option.Type.Inet:
					break;
				case Option.Type.List:
					ret.option_value = readOption(s, counter);
					break;
				case Option.Type.Map:
					ret.key_values_option_value[0] = readOption(s, counter);
					ret.key_values_option_value[1] = readOption(s, counter);
					break;
				case Option.Type.Set:
					ret.option_value = readOption(s, counter);
					break;
			}
			return ret;
		}

		struct ColumnSpecification {
			string ksname;
			string tablename;

			string name;
			Option type;
		}
		auto readColumnSpecification(bool hasGlobalTablesSpec) {
			ColumnSpecification ret;
			if (!hasGlobalTablesSpec) {
				ret.ksname = readShortString(sock, counter);
				ret.tablename = readShortString(sock, counter);
			}
			ret.name = readShortString(sock, counter);
			ret.type = *readOption(sock, counter);
			return ret;
		}
		auto readColumnSpecifications(bool hasGlobalTablesSpec, int column_count) {
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
		auto readRowsContent(FrameHeader fh, MetaData md) {
			int count;
			auto tmp = readIntNotNULL(count, sock, counter);
			ReturnType!readRowContent[] ret;
			//log("reading %d rows", count);
			for (int i=0; i<count; i++) {
				ret ~= readRowContent(fh, md);
			}
			return ret;
		}
		auto readRowContent(FrameHeader fh, MetaData md) {
			ubyte[][] ret;
			for (int i=0; i<md.columns_count; i++) {
				//log("reading index[%d], %s", i, md.column_specs[i]);
				final switch (md.column_specs[i].type.id) {
					case Option.Type.Custom:
						log("warning column %s has custom type", md.column_specs[i].name);
						ret ~= readIntBytes(sock, counter);
						break;
					case Option.Type.Counter:
						ret ~= readIntBytes(sock, counter);
						throw new Exception("Read Counter Type has not been checked this is what we got: "~ cast(string)ret[$-1]);
						//break;
					case Option.Type.Decimal:
						auto twobytes = readRawBytes(sock, counter, 2);
						if (twobytes == [0xff,0xff]) {
							twobytes = readRawBytes(sock, counter, 2);
							ret ~= null;
							break;
						}
						if (twobytes[0]==0x01 && twobytes[1]==0x01) {
							ret ~= readIntBytes(sock, counter);
						} else {
							auto writer = appender!string();
							formattedWrite(writer, "%s", twobytes);
							throw new Exception("New kind of decimal encountered"~ writer.data);
						}
						break;
					case Option.Type.Boolean:
						ret ~= readRawBytes(sock, counter, int.sizeof);
						break;

					case Option.Type.Ascii:
						goto case;
					case Option.Type.Bigint:
						goto case;
					case Option.Type.Blob:
						goto case;
					case Option.Type.Double:
						goto case;
					case Option.Type.Float:
						goto case;
					case Option.Type.Int:
						goto case;
					version (CassandraV2) {} else {
					case Option.Type.Text:
						goto case;
					}
					case Option.Type.Timestamp:
						goto case;
					case Option.Type.Uuid:
						goto case;
					case Option.Type.Varchar:
						goto case;
					case Option.Type.Varint:
						goto case;
					case Option.Type.Timeuuid:
						goto case;
					case Option.Type.Inet:
						goto case;
					case Option.Type.List:
						goto case;
					case Option.Type.Map:
						goto case;
					case Option.Type.Set:
						ret ~= readIntBytes(sock, counter);
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
		string readSet_keyspace(FrameHeader fh) {
			assert(kind_ is Kind.Set_keyspace);
			return readShortString(sock, counter);
		}

		/**
		 *4.2.5.4. Prepared
		 *
		 *  The result to a PREPARE message. The rest of the body of a Prepared result is:
		 *    V1: <id><metadata>
		 *	  V2: <id><metadata><result_metadata>
		 *  V1: where:
		 *    - <id> is [short bytes] representing the prepared query ID.
		 *    - <metadata> is defined exactly as for a Rows RESULT (See section 4.2.5.2).
		 *  V2: where:
		 *    - <id> is [short bytes] representing the prepared query ID.
		 *    - <metadata> is defined exactly as for a Rows RESULT (See section 4.2.5.2; you
		 *    	can however assume that the Has_more_pages flag is always off) and
		 *    	is the specification for the variable bound in this prepare statement.
		 *    - <result_metadata> is defined exactly as <metadata> but correspond to the
		 *    	metadata for the resultSet that execute this query will yield. Note that
		 *    	<result_metadata> may be empty (have the No_metadata flag and 0 columns, See
		 *    	section 4.2.5.2) and will be for any query that is not a Select. There is
		 *    	in fact never a guarantee that this will non-empty so client should protect
		 *    	themselves accordingly. The presence of this information is an
		 *    	optimization that allows to later execute the statement that has been
		 *    	prepared without requesting the metadata (Skip_metadata flag in EXECUTE).
		 *    	Clients can safely discard this metadata if they do not want to take
		 *    	advantage of that optimization.
		 *
		 *  Note that prepared query ID return is global to the node on which the query
		 *  has been prepared. It can be used on any connection to that node and this
		 *  until the node is restarted (after which the query must be reprepared).
		 */
		void readPrepared(FrameHeader fh) {
			assert(false, "Not a Prepare Result class type");
		}

		/**
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
			CREATED = "CREATED", UPDATED = "UPDATED", DROPPED = "DROPPED"
		}
		Change lastChange_; string lastchange() { return lastChange_; }
		string currentKeyspace_; string keyspace() { return currentKeyspace_; }
		string currentTable_; string table() { return currentTable_; }
		void readSchema_change(FrameHeader fh) {
			assert(kind_ is Kind.Schema_change);

			lastChange_ = cast(Change)readShortString(sock, counter);

			currentKeyspace_ = readShortString(sock, counter);
			currentTable_ = readShortString(sock, counter);
		}
	}
	class PreparedStatement : Result {
		ubyte[] id;
		Consistency consistency = Consistency.ANY;
		ubyte flags;
		int page_size = -1;
		byte[] paging_state;
		Consistency serial_consistency = Consistency.SERIAL;

		MetaData result_metadata;

		this(FrameHeader fh) {
			super(fh);
			if (kind != Result.Kind.Prepared) {
				throw new Exception("CQLProtocolException, Unknown result type for PREPARE command");
			}
		}

		/// See section 4.2.5.4.
		override void readPrepared(FrameHeader fh) {
			assert(kind_ is Kind.Prepared);
			id = readShortBytes(sock, counter);
			metadata = readRowMetaData(fh);
			version (CassandraV2) {
				result_metadata = readRowMetaData(fh);
			}
		}

		Result execute(Args...)(Args args) {
			version (CassandraV2) {
						return Connection.execute(id, consistency, flags, page_size, paging_state, serial_consistency, args);
			} else {
						return Connection.execute(id, consistency, args);
			}
		}

		override
		string toString() {
			return "PreparedStatement("~id.hex~")";
		}
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
	enum Event :string {
		TOPOLOGY_CHANGE = "TOPOLOGY_CHANGE",
		STATUS_CHANGE = "STATUS_CHANGE",
		SCHEMA_CHANGE = "SCHEMA_CHANGE",
		NEW_NODE = "NEW_NODE",
		UP = "UP"
	}
	void readEvent(FrameHeader fh) {
		assert(fh.isEVENT);
	}
	/*void writeEvents(Appender!(ubyte[]) appender, Event e...) {
		appender.append(e);
	}*/

	/** 4.2.7. AUTH_CHALLENGE
	 *
	 *   A server authentication challenge (see AUTH_RESPONSE (Section 4.1.2) for more
	 *   details).
	 *
	 *   The body of this message is a single [bytes] token. The details of what this
	 *   token contains (and when it can be null/empty, if ever) depends on the actual
	 *   authenticator used.
	 *
	 *   Clients are expected to answer the server challenge by an AUTH_RESPONSE
	 *   message.
	 */
	ubyte[] readAuthChallenge() {
		return readIntBytes(sock, counter);
	}

	/** 4.2.7. AUTH_SUCCESS
	 *
	 *   Indicate the success of the authentication phase. See Section 4.2.3 for more
	 *   details.
	 *
	 *   The body of this message is a single [bytes] token holding final information
	 *   from the server that the client may require to finish the authentication
	 *   process. What that token contains and whether it can be null depends on the
	 *   actual authenticator used.
	 */
	ubyte[] readAuthSuccess() {
		return readIntBytes(sock, counter);
	}



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
	 *
	 * V2:	As of this version 2 of the protocol, the following compressions are available:
	 *		    - lz4 (https://code.google.com/p/lz4/). In that, note that the 4 first bytes
	 *		      of the body will be the uncompressed length (followed by the compressed
	 *		      bytes).
	 *		    - snappy (https://code.google.com/p/snappy/). This compression might not be
	 *		      available as it depends on a native lib (server-side) that might not be
	 *		      avaivable on some installation.
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
	auto readList(T)(FrameHeader fh) {
		auto size = readShort(fh);
		T[] ret;
		foreach (i; 0..size) {
			ret ~= readBytes!T(sock, counter);
		}
		return ret;

	}

	/**     Map: a [short] n indicating the size of the map, followed by n entries.
	 *          Each entry is composed of two [short bytes] representing the key and
	 *          the value of the entry map.
	 */
	auto readMap(T,U)(FrameHeader fh) {
		auto size = readShort(fh);
		T[U] ret;
		foreach (i; 0..size) {
			ret[readShortBytes!T(sock, counter)] = readShortBytes!U(sock,counter);

		}
		return ret;
	}

	/**     Set: a [short] n indicating the size of the set, followed by n elements.
	 *          Each element is [short bytes] representing the serialized element
	 *          value.
	 */
	auto readSet(T)(FrameHeader fh) {
		auto size = readShort(fh);
		T[] ret;
		foreach (i; 0..size) {
			ret[] ~= readBytes!T(sock, counter);

		}
		return ret;
	}

	/**
	 * 7. Result paging
	 *
	 *   The protocol allows for paging the result of queries. For that, the QUERY and
	 *   EXECUTE messages have a <result_page_size> value that indicate the desired
	 *   page size in CQL3 rows.
	 *
	 *   If a positive value is provided for <result_page_size>, the result set of the
	 *   RESULT message returned for the query will contain at most the
	 *   <result_page_size> first rows of the query result. If that first page of result
	 *   contains the full result set for the query, the RESULT message (of kind `Rows`)
	 *   will have the Has_more_pages flag *not* set. However, if some results are not
	 *   part of the first response, the Has_more_pages flag will be set and the result
	 *   will contain a <paging_state> value. In that case, the <paging_state> value
	 *   should be used in a QUERY or EXECUTE message (that has the *same* query than
	 *   the original one or the behavior is undefined) to retrieve the next page of
	 *   results.
	 *
	 *   Only CQL3 queries that return a result set (RESULT message with a Rows `kind`)
	 *   support paging. For other type of queries, the <result_page_size> value is
	 *   ignored.
	 *
	 *   Note to client implementors:
	 *   - While <result_page_size> can be as low as 1, it will likely be detrimental
	 *     to performance to pick a value too low. A value below 100 is probably too
	 *     low for most use cases.
	 *   - Clients should not rely on the actual size of the result set returned to
	 *     decide if there is more result to fetch or not. Instead, they should always
	 *     check the Has_more_pages flag (unless they did not enabled paging for the query
	 *     obviously). Clients should also not assert that no result will have more than
	 *     <result_page_size> results. While the current implementation always respect
	 *     the exact value of <result_page_size>, we reserve ourselves the right to return
	 *     slightly smaller or bigger pages in the future for performance reasons.
	 */



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
	 typedef string WriteType;
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
		ServerError = 0x0000,
		ProtocolError = 0x000A,
		BadCredentials = 0x0100,
		UnavailableException = 0x1000,
		Overloaded = 0x1001,
		IsBootstrapping = 0x1002,
		TruncateError = 0x1003,
		WriteTimeout = 0x1100,
		ReadTimeout = 0x1200,
		SyntaxError = 0x2000,
		Unauthorized = 0x2100,
		Invalid = 0x2200,
		ConfigError = 0x2300,
		AlreadyExists = 0x2400,
		Unprepared = 0x2500
	 }
	 string toString(Error err) {
		switch (err) {
			case Error.ServerError:
				return "Server error: something unexpected happened. This indicates a server-side bug.";
			case Error.ProtocolError:
				return "Protocol error: some client message triggered a protocol violation (for instance a QUERY message is sent before a STARTUP one has been sent)";
			case Error.BadCredentials:
				return "Bad credentials: CREDENTIALS request failed because Cassandra did not accept the provided credentials.";
			case Error.UnavailableException:
				return "Unavailable exception.";
			case Error.Overloaded:
				return "Overloaded: the request cannot be processed because the coordinator node is overloaded";
			case Error.IsBootstrapping:
				return "Is_bootstrapping: the request was a read request but the coordinator node is bootstrapping";
			case Error.TruncateError:
				return "Truncate_error: error during a truncation error.";
			case Error.WriteTimeout:
				return "Write_timeout: Timeout exception during a write request.";
			case Error.ReadTimeout:
				return "Read_timeout: Timeout exception during a read request.";
			case Error.SyntaxError:
				return "Syntax_error: The submitted query has a syntax error.";
			case Error.Unauthorized:
				return "Unauthorized: The logged user doesn't have the right to perform the query.";
			case Error.Invalid:
				return "Invalid: The query is syntactically correct but invalid.";
			case Error.ConfigError:
				return "Config_error: The query is invalid because of some configuration issue.";
			case Error.AlreadyExists:
				return "Already_exists: The query attempted to create a keyspace or a table that was already existing.";
			case Error.Unprepared:
				return "Unprepared: Can be thrown while a prepared statement tries to be executed if the provide prepared statement ID is not known by this host.";
			default:
				assert(false);
		}
	 }
}

/** 9. Changes from v1
 *  * Protocol is versioned to allow old client connects to a newer server, if a
 *    newer client connects to an older server, it needs to check if it gets a
 *    ProtocolException on connection and try connecting with a lower version.
 *  * A query can now have bind variables even though the statement is not
 *    prepared; see Section 4.1.4.
 *  * A new BATCH message allows to batch a set of queries (prepared or not); see 
 *    Section 4.1.7.
 *  * Authentication now uses SASL. Concretely, the CREDENTIALS message has been
 *    removed and replaced by a server/client challenges/responses exchanges (done
 *    through the new AUTH_RESPONSE/AUTH_CHALLENGE messages). See Section 4.2.3 for
 *    details.
 *  * Query paging has been added (Section 7): QUERY and EXECUTE message have an
 *    additional <result_page_size> [int] and <paging_state> [bytes], and
 *    the Rows kind of RESULT message has an additional flag and <paging_state> 
 *    value. Note that paging is optional, and a client that do not want to handle
 *    can simply avoid including the Page_size flag and parameter in QUERY and
 *    EXECUTE.
 *  * QUERY and EXECUTE statements can request for the metadata to be skipped in
 *    the result set returned (for efficiency reasons) if said metadata are known
 *    in advance. Furthermore, the result to a PREPARE (section 4.2.5.4) now
 *    includes the metadata for the result of executing the statement just
 *    prepared (though those metadata will be empty for non SELECT statements).
 *
 * ==============================END SPEC============================
 */

class Authenticator {
	StringMap getCredentials() {
		StringMap ret;
		return ret;
	}
}
Authenticator GetAuthenticator(string type) {
	// TODO: provide real authenticator types that work with the ones in Cassandra
	return new Authenticator();
}





class CQLException : Exception {
	this(string s = "", string file=__FILE__, int line=__LINE__) {
		super(s, file, line);
	}
}

class CQLProtocolException : CQLException {
	this(string s = "", string file=__FILE__, int line=__LINE__) {
		super(s, file, line);
	}
}
class NotImplementedException : CQLException {
	this(string s = "Not Implemented", string file=__FILE__, int line=__LINE__) {
		super(s, file, line);
	}
}


string bestCassandraType(T)() {
	import std.datetime;

	static if (is(T == bool)) {
		return "boolean";
	} else static if (is(T == int)) {
		return "int";
	} else static if (is (T == long)) {
		return "bigint";
	} else static if (is (T == float)) {
		return "float";
	} else static if (is (T == double)) {
		return "double";
	} else static if (is (T == string)) {
		return "text";
	} else static if (is (T == ubyte[])) {
		return "blob";
	} else static if (is (T == InetAddress)) {
		return "inet";
	} else static if (is (T == InetAddress6)) {
		return "inet";
	} else static if (is (T == DateTime)) {
		return "timestamp";
	} else {
		assert(0, "Can't suggest a cassandra cql type for storing: "~T.stringof);
	}

}




unittest {
	auto cassandra = new Connection();
	cassandra.connect("127.0.0.1", 9042);
	scope(exit) cassandra.close();

	auto opts = cassandra.requestOptions();
	foreach (opt, values; opts) {
		writeln(opt, values);
	}

	try {
		writefln("USE twissandra");
		auto res = cassandra.query(`USE twissandra`, Consistency.ANY);
		writefln("using %s %s", res.kind_, res.keyspace);
	} catch (Exception e) {

		try {
			writefln("CREATE KEYSPACE twissandra");
			auto res = cassandra.query(`CREATE KEYSPACE twissandra WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`, Consistency.ANY);
			writefln("created %s %s %s", res.kind_, res.keyspace, res.lastchange);
		} catch (Exception e) {writefln(e.msg);}
	}


	try {
		writefln("CREATE TABLE ");
		auto res = cassandra.query(`CREATE TABLE users (
				user_name varchar,
				password varchar,
				gender varchar,
				session_token varchar,
				state varchar,
				birth_year bigint,
				PRIMARY KEY (user_name)
			  )`, Consistency.ANY);
		writefln("created table %s %s %s %s", res.kind_, res.keyspace, res.lastchange, res.table);
	} catch (Exception e) {writefln(e.msg);}

	try {
		writefln("INSERT");
		assert(cassandra.insert(`INSERT INTO users
				(user_name, password)
				VALUES ('jsmith', 'ch@ngem3a')`));
		writefln("inserted");
	} catch (Exception e) {writefln(e.msg);}

	try {
		writefln("SELECT");
		auto res = cassandra.select(`SELECT * FROM users WHERE user_name='jsmith'`);
		writefln("select resulted in %s\n%s", res.kind_, res);
	} catch (Exception e) {writeln(e);}


	try {
		writefln("CREATE TABLE ");
		auto res = cassandra.query(`CREATE TABLE alltypes (
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
			  )`, Consistency.ANY);
		writefln("created table %s %s %s %s", res.kind_, res.keyspace, res.lastchange, res.table);
	} catch (Exception e) {writefln(e.msg);}

	try {
		writefln("INSERT into alltypes");
		assert(cassandra.insert(`INSERT INTO alltypes (user_name,birth_year,ascii_col,blob_col,booleant_col, booleanf_col,decimal_col,double_col,float_col,inet_col,int_col,list_col,map_col,set_col,text_col,timestamp_col,uuid_col,timeuuid_col,varint_col)
				VALUES ('bob@domain.com', 7777777777,
					'someasciitext', 0x2020202020202020202020202020,
					True, False,
					 123, 8.5, 9.44, '127.0.0.1', 999,
					['li1','li2','li3'], {'blurg':'blarg'}, { 'kitten', 'cat', 'pet' },
					'some text col value', 'now', aaaaaaaa-eeee-cccc-9876-dddddddddddd,
					 now(),
					9494949449
					)`));
		writefln("inserted");
	} catch (Exception e) {writefln(e.msg);}


	try {
		writefln("PREPARE INSERT into alltypes");
		auto stmt = cassandra.prepare(`INSERT INTO alltypes (user_name,birth_year, ascii_col, blob_col, booleant_col, booleanf_col, decimal_col, double_col
				, float_col, inet_col, int_col, list_col, map_col, set_col, text_col, timestamp_col
				, uuid_col, timeuuid_col, varint_col)`
				` VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
		writeln(stmt);
		alias long Bigint;
		alias double Double;
		alias float Float;
		alias int InetAddress4;
		alias ubyte[16] InetAddress16;
		alias long Timestamp;

		stmt.execute("rory", cast(Bigint)1378218642, "mossesf asciiiteeeext", 0x898989898989
			, true, false, cast(long)999, cast(double)8.88, cast(float)7.77, cast(int)2130706433
			, 66666, ["thr","for"], ["key1": "value1"], ["one","two", "three"], "some more text«»"
			, cast(Timestamp)0x0000021212121212, "\xaa\xaa\xaa\xaa\xee\xee\xcc\xcc\x98\x76\xdd\xdd\xdd\xdd\xdd\xdd"
			, "\xb3\x8b\x6d\xb0\x14\xcc\x11\xe3\x81\x03\x9d\x48\x04\xae\x88\xb3", long.max);
	} catch (Exception e) {writefln(e.msg);} // list should be:  [0, 2, 0, 3, 111, 110, 101, 0, 3, 116, 119, 111]

	try {
		writefln("SELECT from alltypes");
		auto res = cassandra.select(`SELECT * FROM alltypes`);
		auto rows = res.rows();
		writefln("got %d rows", rows.length);
		writeln(rows[0]);
		writefln("select resulted in Result.Kind.%s\n%s", res.kind, res);
	} catch (Exception e) {writeln(e);}


	cassandra.close();
	writeln("done. exiting");
}


void log(Args...)(string s, Args args) {
	writefln(s, args);
}