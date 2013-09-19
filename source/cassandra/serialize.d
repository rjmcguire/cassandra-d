

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


// some types
unittest {
	assert(int.sizeof == 4, "int is not 32 bits"~ to!string(int.sizeof));
}
/**
 *                             CQL BINARY PROTOCOL v1
 *
 *
 *Table of Contents
 *
 *  1. Overview
 *  2. Frame header
 *    2.1. version
 *    2.2. flags
 *    2.3. stream
 *    2.4. opcode
 *    2.5. length
 *  3. Notations
 *  4. Messages
 *    4.1. Requests
 *      4.1.1. STARTUP
 *      4.1.2. CREDENTIALS
 *      4.1.3. OPTIONS
 *      4.1.4. QUERY
 *      4.1.5. PREPARE
 *      4.1.6. EXECUTE
 *      4.1.7. REGISTER
 *    4.2. Responses
 *      4.2.1. ERROR
 *      4.2.2. READY
 *      4.2.3. AUTHENTICATE
 *      4.2.4. SUPPORTED
 *      4.2.5. RESULT
 *        4.2.5.1. Void
 *        4.2.5.2. Rows
 *        4.2.5.3. Set_keyspace
 *        4.2.5.4. Prepared
 *        4.2.5.5. Schema_change
 *      4.2.6. EVENT
 *  5. Compression
 *  6. Collection types
 *  7. Error codes
 *
 *
 *1. Overview
 *
 *  The CQL binary protocol is a frame based protocol. Frames are defined as:
 *
 *      0         8        16        24        32
 *      +---------+---------+---------+---------+
 *      | version |  flags  | stream  | opcode  |
 *      +---------+---------+---------+---------+
 *      |                length                 |
 *      +---------+---------+---------+---------+
 *      |                                       |
 *      .            ...  body ...              .
 *      .                                       .
 *      .                                       .
 *      +----------------------------------------
 *
 *  The protocol is big-endian (network byte order).
 *
 *  Each frame contains a fixed size header (8 bytes) followed by a variable size
 *  body. The header is described in Section 2. The content of the body depends
 *  on the header opcode value (the body can in particular be empty for some
 *  opcode values). The list of allowed opcode is defined Section 2.3 and the
 *  details of each corresponding message is described Section 4.
 *
 *  The protocol distinguishes 2 types of frames: requests and responses. Requests
 *  are those frame sent by the clients to the server, response are the ones sent
 *  by the server. Note however that while communication are initiated by the
 *  client with the server responding to request, the protocol may likely add
 *  server pushes in the future, so responses does not obligatory come right after
 *  a client request.
 *
 *  Note to client implementors: clients library should always assume that the
 *  body of a given frame may contain more data than what is described in this
 *  document. It will however always be safe to ignore the remaining of the frame
 *  body in such cases. The reason is that this may allow to sometimes extend the
 *  protocol with optional features without needing to change the protocol
 *  version.
 *
 *
 *2. Frame header
 */
struct FrameHeader {

	/**
	 *2.1. version
	 *
	 *  The version is a single byte that indicate both the direction of the message
	 *  (request or response) and the version of the protocol in use. The up-most bit
	 *  of version is used to define the direction of the message: 0 indicates a
	 *  request, 1 indicates a responses. This can be useful for protocol analyzers to
	 *  distinguish the nature of the packet from the direction which it is moving.
	 *  The rest of that byte is the protocol version (1 for the protocol defined in
	 *  this document). In other words, for this version of the protocol, version will
	 *  have one of:
	 *    0x01    Request frame for this protocol version
	 *    0x81    Response frame for this protocol version
	 */
	enum Version : ubyte {
		V1Request = 0x01,
		V1Response =  0x81,
		V2Request = 0x02,
		V2Response = 0x82
	}
	version (CassandraV2) {
		Version version_ = Version.V2Request;
	} else {
		Version version_ = Version.V1Request;
	}

	/**
	 *2.2. flags
	 *
	 *  Flags applying to this frame. The flags have the following meaning (described
	 *  by the mask that allow to select them):
	 *    0x01: Compression flag. If set, the frame body is compressed. The actual
	 *          compression to use should have been set up beforehand through the
	 *          Startup message (which thus cannot be compressed; Section 4.1.1).
	 *    0x02: Tracing flag. For a request frame, this indicate the client requires
	 *          tracing of the request. Note that not all requests support tracing.
	 *          Currently, only QUERY, PREPARE and EXECUTE queries support tracing.
	 *          Other requests will simply ignore the tracing flag if set. If a
	 *          request support tracing and the tracing flag was set, the response to
	 *          this request will have the tracing flag set and contain tracing
	 *          information.
	 *          If a response frame has the tracing flag set, its body contains
	 *          a tracing ID. The tracing ID is a [uuid] and is the first thing in
	 *          the frame body. The rest of the body will then be the usual body
	 *          corresponding to the response opcode.
	 *
	 *  The rest of the flags is currently unused and ignored.
	 */
	mixin(bitfields!(
		bool,"compress", 1,
		bool,"trace", 1,
		uint, "", 6
		));
	bool hasTracing() { if (this.trace) return true; return false; }

	/**2.3. stream
	 *
	 *  A frame has a stream id (one signed byte). When sending request messages, this
	 *  stream id must be set by the client to a positive byte (negative stream id
	 *  are reserved for streams initiated by the server; currently all EVENT messages
	 *  (section 4.2.6) have a streamId of -1). If a client sends a request message
	 *  with the stream id X, it is guaranteed that the stream id of the response to
	 *  that message will be X.
	 *
	 *  This allow to deal with the asynchronous nature of the protocol. If a client
	 *  sends multiple messages simultaneously (without waiting for responses), there
	 *  is no guarantee on the order of the responses. For instance, if the client
	 *  writes REQ_1, REQ_2, REQ_3 on the wire (in that order), the server might
	 *  respond to REQ_3 (or REQ_2) first. Assigning different stream id to these 3
	 *  requests allows the client to distinguish to which request an received answer
	 *  respond to. As there can only be 128 different simultaneous stream, it is up
	 *  to the client to reuse stream id.
	 *
	 *  Note that clients are free to use the protocol synchronously (i.e. wait for
	 *  the response to REQ_N before sending REQ_N+1). In that case, the stream id
	 *  can be safely set to 0. Clients should also feel free to use only a subset of
	 *  the 128 maximum possible stream ids if it is simpler for those
	 *  implementation.
	 */
	byte streamid; bool isServerStream() { if (streamid < 0) return true; return false; } bool isEvent() { if (streamid==-1) return true; return false;}

	/**2.4. opcode
	 *
	 *  An integer byte that distinguish the actual message:
	 *    0x00    ERROR
	 *    0x01    STARTUP
	 *    0x02    READY
	 *    0x03    AUTHENTICATE
	 *    0x04    CREDENTIALS // Gone in V2
	 *    0x05    OPTIONS
	 *    0x06    SUPPORTED
	 *    0x07    QUERY
	 *    0x08    RESULT
	 *    0x09    PREPARE
	 *    0x0A    EXECUTE
	 *    0x0B    REGISTER
	 *    0x0C    EVENT
	 *
	 *  Messages are described in Section 4.
	 */
	enum OpCodeV1 : byte {
		ERROR,
		STARTUP,
		READY,
		AUTHENTICATE,
		CREDENTIALS,
		OPTIONS,
		SUPPORTED,
		QUERY,
		RESULT,
		PREPARE,
		EXECUTE,
		REGISTER,
		EVENT
	};
	enum OpCodeV2 : byte {
		ERROR = 0x00,
		STARTUP = 0x01,
		READY = 0x02,
		AUTHENTICATE = 0x03,
		// NOTE THAT CREDENTIALS = 0x04 is no longer valid in v2
		OPTIONS = 0x05,
		SUPPORTED = 0x06,
		QUERY = 0x07,
		RESULT = 0x08,
		PREPARE = 0x09,
		EXECUTE = 0x0A,
		REGISTER = 0x0B,
		EVENT = 0x0C,
		BATCH = 0x0D,
		AUTH_CHALLENGE = 0x0E,
		AUTH_RESPONSE = 0x0F,
		AUTH_SUCCESS = 0x10,
	};
	version (CassandraV2)
		alias OpCodeV2 OpCode;
	else
		alias OpCodeV1 OpCode;

	OpCode opcode;
	bool isERROR() { if (opcode == OpCode.ERROR) return true; return false; }
	bool isSTARTUP() { if (opcode == OpCode.STARTUP) return true; return false; }
	bool isREADY() { if (opcode == OpCode.READY) return true; return false; }
	bool isAUTHENTICATE() { if (opcode == OpCode.AUTHENTICATE) return true; return false; }
	version (CassandraV2) {} else bool isCREDENTIALS() { if (opcode == OpCode.CREDENTIALS) return true; return false; }
	bool isOPTIONS() { if (opcode == OpCode.OPTIONS) return true; return false; }
	bool isSUPPORTED() { if (opcode == OpCode.SUPPORTED) return true; return false; }
	bool isQUERY() { if (opcode == OpCode.QUERY) return true; return false; }
	bool isRESULT() { if (opcode == OpCode.RESULT) return true; return false; }
	bool isPREPARE() { if (opcode == OpCode.PREPARE) return true; return false; }
	bool isEXECUTE() { if (opcode == OpCode.EXECUTE) return true; return false; }
	bool isREGISTER() { if (opcode == OpCode.REGISTER) return true; return false; }
	bool isEVENT() { if (opcode == OpCode.EVENT) return true; return false; }
	version (CassandraV2) {
		bool isBATCH() { if (opcode == OpCode.BATCH) return true; return false; }
		bool isAUTH_CHALLENGE() { if (opcode == OpCode.AUTH_CHALLENGE) return true; return false; }
		bool isAUTH_RESPONSE() { if (opcode == OpCode.AUTH_RESPONSE) return true; return false; }
		bool isAUTH_SUCCESS() { if (opcode == OpCode.AUTH_SUCCESS) return true; return false; }
	}


	/**
	 *2.5. length
	 *
	 *  A 4 byte integer representing the length of the body of the frame (note:
	 *  currently a frame is limited to 256MB in length).
	 */
	int length;


	ubyte[] bytes() {
		import std.bitmanip : write;
		import std.array : appender;
		auto buffer = appender!(ubyte[])();
		foreach (i,v; this.tupleof) {
			if (is( typeof(v) : int )) {
				ubyte[] buf = [0,0,0,0,0,0,0,0];
				buf.write!(typeof(v))(v, 0);
				buffer.put(buf[0..typeof(v).sizeof]);
			}
		}
		return buffer.data;
	}
}


int getIntLength(Appender!(ubyte[]) appender) {
	assert(appender.data.length < int.max);
	return cast(int)appender.data.length;
}

FrameHeader readFrameHeader(TcpConnection s, ref int counter) {
	assert(counter == 0, to!string(counter) ~" bytes unread from last Frame");
	counter = int.max;
	writefln("===================read frame header========================");
	auto fh = FrameHeader();
	fh.version_ = cast(FrameHeader.Version)readByte(s, counter);
	version (CassandraV2) {
		assert(fh.version_ == FrameHeader.Version.V2Response);
	} else {
		assert(fh.version_ == FrameHeader.Version.V1Response);
	}
	readByte(s, counter); // FIXME: this should load into flags
	fh.streamid = readByte(s, counter);
	fh.opcode = cast(FrameHeader.OpCode)readByte(s, counter);
	readIntNotNULL(fh.length, s, counter);

	counter = fh.length;
	writefln("=================== end read frame header===================");
	//writefln("go %d data to play", counter);

	return fh;
}
byte readByte(TcpConnection s, ref int counter) {
	ubyte[1] buf;
	auto tmp = buf[0..$];
	s.read(tmp);
	counter--;
	return buf[0];
}

/**
 *3. Notations
 *
 *  To describe the layout of the frame body for the messages in Section 4, we
 *  define the following:
 *
 *    [int]          A 4 bytes integer
 */
int* readInt(ref int ptr, TcpConnection s, ref int counter) {
	import std.bitmanip : read;
	ubyte[int.sizeof] buffer;
	auto tmp = buffer[0..$];
	s.read(tmp);
	auto buf = buffer[0..int.sizeof];

	ptr = buf.read!int();
	//writefln("readInt %d %s", ptr, buffer);
	/*if (r >= int.max) while (true) {
		buffer[] = [0,0,0,0];
		auto n1 = s.read(buffer);
		writefln("readInt bork %s bytes:%d", buffer, n1);
		buf = buffer[0..int.sizeof];
		r = buf.read!uint();
	}*/
	counter -= int.sizeof;
	if (ptr == -1) {
		//throw new Exception("NULL");
		return null;
	}
	return &ptr;
}
int readIntNotNULL(ref int ptr, TcpConnection s, ref int counter) {
	auto tmp = readInt(ptr, s, counter);
	if (tmp is null) throw new Exception("NullException");
	return *tmp;
}

///    [short]        A 2 bytes unsigned integer
short readShort(TcpConnection s, ref int counter) {
	import std.bitmanip : read;
	ubyte[short.sizeof] buffer;
	auto tmp = buffer[];
	s.read(tmp);
	auto buf = buffer[0..short.sizeof];
	auto r = buf.read!ushort();
	//writefln("readShort %d @ %d", r, counter);
	counter -= short.sizeof;
	return cast(short)r;
}

 /**    [string]       A [short] n, followed by n bytes representing an UTF-8
 *                   string.
 */
ubyte[] readRawBytes(TcpConnection s, ref int counter, int len) {
	ubyte[] buf = new ubyte[](len);
	auto tmp = buf[];
	s.read(tmp);
	counter -= buf.length;
	return buf;
}
string readShortString(TcpConnection s, ref int counter) {
	auto len = readShort(s, counter);
	if (len < 0) { return null; }

	//writefln("readString %d @ %d", len, counter);
	auto bytes = readRawBytes(s, counter, len);
	string str = cast(string)bytes[0..len];
	return str;
}

///    [long string]  An [int] n, followed by n bytes representing an UTF-8 string.
string readLongString(TcpConnection s, ref int counter) {
	int len;
	auto tmp = readInt(len, s, counter);
	if (tmp is null) { return null; }

	writefln("readString %d @ %d", len, counter);
	auto bytes = readRawBytes(s, counter, len);
	string str = cast(string)bytes[0..len];
	return str;
}


/**    [uuid]         A 16 bytes long uuid.
 *    [string list]  A [short] n, followed by n [string].
 */
alias string[] StringList;
StringList readStringList(TcpConnection s, ref int counter) {
	StringList ret;
	auto len = readShort(s, counter);

	for (int i=0; i<len && counter>0; i++) {
		ret ~= readShortString(s, counter);
	}
	if (ret.length < len && counter <= 0)
		throw new Exception("ran out of data");
	return ret;
}

/**    [bytes]        A [int] n, followed by n bytes if n >= 0. If n < 0,
 *                   no byte should follow and the value represented is `null`.
 */
ubyte[] readIntBytes(TcpConnection s, ref int counter) {
	int len;
	auto tmp = readInt(len, s, counter);
	if (tmp is null) {
		//writefln("reading (null) bytes");
		return null;
	}
	//writefln("reading int(%d) bytes", len);


	ubyte[] buf = new ubyte[](len);
	s.read(buf);
	//writefln("got bytes: %s", cast(char[])buf);
	counter -= buf.length;
	return buf;
}
auto appendIntBytes(T)(Appender!(ubyte[]) appender, T data) {
	static if (is(T == string) || is(T == ubyte[]) || is(T == byte[])) {
		assert(data.length < int.max);
		append(appender, cast(int)data.length);
		append(appender, cast(ubyte[])data.ptr[0..data.length]);
	} else static if (isArray!T) {
		assert(data.length < uint.max);
		auto tmpapp = std.array.appender!(ubyte[])();
		tmpapp.appendRawBytes(cast(ushort)data.length);
		foreach (item; data) {
			assert(item.length < ushort.max);
			tmpapp.appendShortBytes(item);
		}
		appender.appendIntBytes(tmpapp.data);
	} else static if (isAssociativeArray!T) {
		assert(data.length < uint.max);
		auto tmpapp = std.array.appender!(ubyte[])();
		tmpapp.appendRawBytes(cast(ushort)data.length);
		foreach (key,value; data) {
			tmpapp.appendShortBytes(key);
			tmpapp.appendShortBytes(value);
		}
		appender.appendIntBytes(tmpapp.data);
	} else static if (isIntegral!T || is(T == double) || is(T == float) || is(T == ushort)) {
		import std.bitmanip : write;

		assert(T.sizeof < int.max);
		ubyte[] buffer = [0, 0, 0, 0, 0, 0, 0, 0];
		append(appender, cast(int)T.sizeof);
		buffer.write!(T)(data,0);
		appender.append(buffer[0..T.sizeof]);
	} else static if (isBoolean!T) {
		import std.bitmanip : write;

		ubyte[] buffer = [0, 0, 0, 0, 0, 0, 0, 0];
		append(appender, cast(int)1);
		if (data)
			buffer.write!(byte)(1,0);
		else
			buffer.write!(byte)(0,0);
		appender.append(buffer[0 .. 1]);
	} else {
		assert(0, "can't append raw bytes for type: "~ T.stringof);
	}
	return appender;
}

/**    [short bytes]  A [short] n, followed by n bytes if n >= 0.
 */
ubyte[] readShortBytes(TcpConnection s, ref int counter) {
	auto len = readShort(s, counter);
	if (len==0) { return null; }

	ubyte[] buf = new ubyte[](len);
	s.read(buf);
	counter -= buf.length;
	return buf;
}
auto appendShortBytes(T)(Appender!(ubyte[]) appender, T data) {
	static if (is (T == ubyte[]) || is (T == string)) {
		assert(data.length < short.max);
		append(appender, cast(short)data.length);


		append(appender, cast(ubyte[])data.ptr[0..data.length]);
	} else {
		assert(0, "appendShortBytes can't append type: "~ T.stringof);
	}
	return appender;
}

/**
 *    [option]       A pair of <id><value> where <id> is a [short] representing
 *                   the option id and <value> depends on that option (and can be
 *                   of size 0). The supported id (and the corresponding <value>)
 *                   will be described when this is used.
 */
struct Option {
	/// See Section: 4.2.5.2.
	version (CassandraV2) {
		enum Type {
			Custom = 0x0000,
			Ascii = 0x0001,
			Bigint = 0x0002,
			Blob = 0x0003,
			Boolean = 0x0004,
			Counter = 0x0005,
			Decimal = 0x0006,
			Double = 0x0007,
			Float = 0x0008,
			Int = 0x0009,
			Timestamp = 0x000B,
			Uuid = 0x000C,
			Varchar = 0x000D,
			Varint = 0x000E,
			Timeuuid = 0x000F,
			Inet = 0x0010,
			List = 0x0020,
			Map = 0x0021,
			Set = 0x0022
		}
	} else {
		enum Type {
			Custom = 0x0000,
			Ascii = 0x0001,
			Bigint = 0x0002,
			Blob = 0x0003,
			Boolean = 0x0004,
			Counter = 0x0005,
			Decimal = 0x0006,
			Double = 0x0007,
			Float = 0x0008,
			Int = 0x0009,
			Text = 0x000A,
			Timestamp = 0x000B,
			Uuid = 0x000C,
			Varchar = 0x000D,
			Varint = 0x000E,
			Timeuuid = 0x000F,
			Inet = 0x0010,
			List = 0x0020,
			Map = 0x0021,
			Set = 0x0022
		}
	}
	Type id;
	union {
		string string_value;
		Option* option_value;
		Option*[2] key_values_option_value;
	}

	string toString() {
		auto buf = appender!string();
		formattedWrite(buf, "%s ", id);
		if (id == Option.Type.Custom) {
			formattedWrite(buf, "%s", string_value);
		} else if (id == Option.Type.List || id == Option.Type.Set) {
			formattedWrite(buf, "%s", option_value);
		} else if (id == Option.Type.Map) {
			formattedWrite(buf, "%s[%s]", key_values_option_value[1], key_values_option_value[0]);
		}
		return buf.data;
	}

}


/**    [option list]  A [short] n, followed by n [option].
 *    [inet]         An address (ip and port) to a node. It consists of one
 *                   [byte] n, that represents the address size, followed by n
 *                   [byte] representing the IP address (in practice n can only be
 *                   either 4 (IPv4) or 16 (IPv6)), following by one [int]
 *                   representing the port.
 *    [consistency]  A consistency level specification. This is a [short]
 *                   representing a consistency level with the following
 *                   correspondance:
 *                     0x0000    ANY
 *                     0x0001    ONE
 *                     0x0002    TWO
 *                     0x0003    THREE
 *                     0x0004    QUORUM
 *                     0x0005    ALL
 *                     0x0006    LOCAL_QUORUM
 *                     0x0007    EACH_QUORUM
 *  				   0x0008    SERIAL
 *                     0x0009    LOCAL_SERIAL
 */
enum Consistency : ushort  {
	ANY = 0x0000,
	ONE,
	TWO,
	THREE,
	QUORUM,
	ALL,
	LOCAL_QUORUM,
	EACH_QUORUM,
	SERIAL,
	LOCAL_SERIAL
};

/**
 *    [string map]      A [short] n, followed by n pair <k><v> where <k> and <v> are [string].
 */
typedef string[string] StringMap;

auto append(Args...)(Appender!(ubyte[]) appender, Args args) {
	import std.bitmanip : write;

	ubyte[] buffer = [0, 0, 0, 0, 0, 0, 0, 0];
	//writeln(typeof(args).stringof);
	foreach (arg; args) {
		//writeln(typeof(arg).stringof);
		static if (is(typeof(arg) == ubyte[])) {
			appender.put(arg);
			//writefln("appended type: %s as: %s", typeof(arg).stringof, appender.data[appender.data.length-arg.length..$]);
		} else static if (is(typeof(arg) == ubyte) || is(typeof(arg) == short) || is(typeof(arg) == int) || is(typeof(arg) == long) || is(typeof(arg) == ulong) || is(typeof(arg) == double)) {
			buffer.write!(typeof(arg))(arg,0);
			appender.put(buffer[0..typeof(arg).sizeof]);
			//writefln("appended type: %s as: %s", typeof(arg).stringof, appender.data[appender.data.length-typeof(arg).sizeof..$]);
		} else static if (is(typeof(arg) == string)) {
			assert(arg.length < short.max);
			appender.append(cast(short)arg.length);

			appender.append(cast(ubyte[])arg[0..arg.length]);
			//writefln("appended type: %s as: %s", typeof(arg).stringof, appender.data[appender.data.length-arg.length..$]);
		} else static if (__traits(compiles, mixin("appendOverride(appender, arg)"))) {//hasUFCSmember!(typeof(arg),"toBytes")) {
			auto oldlen = appender.data.length;
			appendOverride(appender, arg);
			//writefln("appended type: %s as: %s", typeof(arg).stringof, appender.data[oldlen..$]);
		} else {
			assert(0, "cannot handle append of "~ typeof(arg).stringof);
		}

	}

	return appender;
}

auto appendRawBytes(T)(Appender!(ubyte[]) appender, T data) {
	import std.bitmanip : write;
	static if (is (T == ushort) || is(T==uint) || is(T==int)) {
		ubyte[] buffer = [0, 0, 0, 0, 0, 0, 0, 0];
		buffer.write!(T)(data,0);
		appender.append(buffer[0..T.sizeof]);
	} else {
		assert(0, "can't use appendRawBytes on type: "~T.stringof);
	}
}

auto appendLongString(Appender!(ubyte[]) appender, string data) {
	assert(data.length < int.max);
	append(appender, cast(int)data.length);


	append(appender, cast(ubyte[])data.ptr[0..data.length]);
	return appender;
}

auto appendOverride(Appender!(ubyte[]) appender, StringMap sm) {
	assert(sm.length < short.max);

	appender.append(cast(short)sm.length);
	foreach (k,v; sm) {
		appender.append(k);
		appender.append(v);
	}
	return appender;
}
auto appendOverride(Appender!(ubyte[]) appender, Consistency c) {
	appender.append(cast(short)c);
}

auto appendOverride(Appender!(ubyte[]) appender, bool b) {
	if (b)
		appender.append(cast(int)0x00000001);
	else
		appender.append(cast(int)0x00000000);
}
auto appendOverride(Appender!(ubyte[]) appender, string[] strs) {
	foreach (str; strs) {
		appender.append(str);
	}
}

/**   [string multimap] A [short] n, followed by n pair <k><v> where <k> is a
 *                      [string] and <v> is a [string list].
 */
alias string[][string] StringMultiMap;
StringMultiMap readStringMultiMap(TcpConnection s, ref int counter) {
	//writefln("got %d to read", counter);
	StringMultiMap smm;
	auto count = readShort(s, counter);
	for (int i=0; i<count && counter>0; i++) {
		auto key = readShortString(s, counter);
		auto values = readStringList(s, counter);
		smm[key] = values;
	}
	if (smm.length < count && counter <= 0)
		throw new Exception("ran out of data to read");

	return smm;
}
