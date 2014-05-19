module cassandra.cql;

import std.array;
import std.bitmanip : bitfields;
import std.conv;
import std.exception : enforce;
import std.format : formattedWrite;
import std.range : isOutputRange;
import std.stdint;
import std.stdio : writeln, writef, writefln;
import std.traits;

import cassandra.internal.utils;
import cassandra.internal.tcpconnection;

// some types
unittest {
	assert(int.sizeof == 4, "int is not 32 bits"~ to!string(int.sizeof));
}
/*
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
 */

/**
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
	Version version_;

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
	 *    0x04    CREDENTIALS
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
	enum OpCode : byte {
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
	OpCode opcode;

	bool isERROR() const pure nothrow { return opcode == OpCode.ERROR; }
	bool isSTARTUP() const pure nothrow { return opcode == OpCode.STARTUP; }
	bool isREADY() const pure nothrow { return opcode == OpCode.READY; }
	bool isAUTHENTICATE() const pure nothrow { return opcode == OpCode.AUTHENTICATE; }
	bool isCREDENTIALS() const pure nothrow { return opcode == OpCode.CREDENTIALS; }
	bool isOPTIONS() const pure nothrow { return opcode == OpCode.OPTIONS; }
	bool isSUPPORTED() const pure nothrow { return opcode == OpCode.SUPPORTED; }
	bool isQUERY() const pure nothrow { return opcode == OpCode.QUERY; }
	bool isRESULT() const pure nothrow { return opcode == OpCode.RESULT; }
	bool isPREPARE() const pure nothrow { return opcode == OpCode.PREPARE; }
	bool isEXECUTE() const pure nothrow { return opcode == OpCode.EXECUTE; }
	bool isREGISTER() const pure nothrow { return opcode == OpCode.REGISTER; }
	bool isEVENT() const pure nothrow { return opcode == OpCode.EVENT; }


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


private int getIntLength(Appender!(ubyte[]) appender) {
	enforce(appender.data.length < int.max);
	return cast(int)appender.data.length;
}

private FrameHeader readFrameHeader(TCPConnection s, ref int counter) {
	assert(counter == 0, to!string(counter) ~" bytes unread from last Frame");
	counter = int.max;
	writefln("===================read frame header========================");
	auto fh = FrameHeader();
	fh.version_ = cast(FrameHeader.Version)readByte(s, counter);
	readByte(s, counter); // FIXME: this should load into flags
	fh.streamid = readByte(s, counter);
	fh.opcode = cast(FrameHeader.OpCode)readByte(s, counter);
	readIntNotNULL(fh.length, s, counter);

	counter = fh.length;
	writefln("=================== end read frame header===================");
	//writefln("go %d data to play", counter);

	return fh;
}
private byte readByte(TCPConnection s, ref int counter) {
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
private int* readInt(ref int ptr, TCPConnection s, ref int counter) {
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
private int readIntNotNULL(ref int ptr, TCPConnection s, ref int counter) {
	auto tmp = readInt(ptr, s, counter);
	if (tmp is null) throw new Exception("NullException");
	return *tmp;
}
/*void write(TCPConnection s, int n) {
	import std.bitmanip : write;

	ubyte[] buffer = [0,0,0,0,0,0,0,0];
	buffer.write!int(n,0);

	if (s.send(buffer[0..n.sizeof]) != n.sizeof) {
		throw new Exception("send failed", s.getErrorText);
	}
	writefln("wrote int %s vs %d", buffer, n);
}*/

///    [short]        A 2 bytes unsigned integer
private short readShort(TCPConnection s, ref int counter) {
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
/*void write(TCPConnection s, short n) {
	import std.bitmanip : write;

	ubyte[] buffer = [0,0,0,0,0,0,0,0];
	buffer.write!short(n,0);

	if (s.send(buffer[0..n.sizeof]) != n.sizeof) {
		throw new Exception("send failed", s.getErrorText);
	}
	writefln("wrote short %s vs %d", buffer, n);
}*/
 /**    [string]       A [short] n, followed by n bytes representing an UTF-8
 *                   string.
 */
private ubyte[] readRawBytes(TCPConnection s, ref int counter, int len) {
	ubyte[] buf = new ubyte[](len);
	auto tmp = buf[];
	s.read(tmp);
	counter -= buf.length;
	return buf;
}
private string readShortString(TCPConnection s, ref int counter) {
	auto len = readShort(s, counter);
	if (len < 0) { return null; }

	//writefln("readString %d @ %d", len, counter);
	auto bytes = readRawBytes(s, counter, len);
	string str = cast(string)bytes[0..len];
	return str;
}
/*void write(TCPConnection s, string str) {
	writeln("writing string");
	if (str.length < short.max) {
		write(s, cast(short)str.length);
	} else if (str.length < int.max) {
		write(s, cast(int)str.length);
	}
	if (s.send(cast(ubyte[])str.ptr[0..str.length]) != str.length) {
		throw new Exception("send failed", s.getErrorText);
	}
	writeln("wrote string");
}*/

///    [long string]  An [int] n, followed by n bytes representing an UTF-8 string.
private string readLongString(TCPConnection s, ref int counter) {
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
private StringList readStringList(TCPConnection s, ref int counter) {
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
private ubyte[] readIntBytes(TCPConnection s, ref int counter) {
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
private auto appendIntBytes(T, R)(R appender, T data)
	if (isOutputRange!(R, ubyte))
{
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
private ubyte[] readShortBytes(TCPConnection s, ref int counter) {
	auto len = readShort(s, counter);
	if (len==0) { return null; }

	ubyte[] buf = new ubyte[](len);
	s.read(buf);
	counter -= buf.length;
	return buf;
}
private auto appendShortBytes(T, R)(R appender, T data)
	if (isOutputRange!(R, ubyte))
{
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
 */
enum Consistency : ushort  {
	ANY = 0x0000,
	ONE,
	TWO,
	THREE,
	QUORUM,
	ALL,
	LOCAL_QUORUM,
	EACH_QUORUM
};

/**
 *    [string map]      A [short] n, followed by n pair <k><v> where <k> and <v> are [string].
 */
alias string[string] StringMap;

private auto append(R, Args...)(R appender, Args args)
	if (isOutputRange!(R, ubyte))
{
	import std.bitmanip : write;

	ubyte[] buffer = [0, 0, 0, 0, 0, 0, 0, 0];
	//writeln(typeof(args).stringof);
	foreach (arg; args) {
		//writeln(typeof(arg).stringof);
		static if (is(typeof(arg) == ubyte[])) {
			appender.put(arg);
			//writefln("appended type: %s as: %s", typeof(arg).stringof, appender.data[appender.data.length-arg.length..$]);
		} else static if (is(typeof(arg) == short) || is(typeof(arg) == int) || is(typeof(arg) == long) || is(typeof(arg) == ulong) || is(typeof(arg) == double)) {
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
//todo: add all the append functions features to append!T(appender,T)
private auto appendRawBytes(T, R)(R appender, T data)
	if (isOutputRange!(R, ubyte))
{
	import std.bitmanip : write;
	static if (is (T == ushort) || is(T==uint) || is(T==int)) {
		ubyte[] buffer = [0, 0, 0, 0, 0, 0, 0, 0];
		buffer.write!(T)(data,0);
		appender.append(buffer[0..T.sizeof]);
	} else {
		assert(0, "can't use appendRawBytes on type: "~T.stringof);
	}
}

private auto appendLongString(R)(R appender, string data)
	if (isOutputRange!(R, ubyte))
{
	assert(data.length < int.max);
	append(appender, cast(int)data.length);


	append(appender, cast(ubyte[])data.ptr[0..data.length]);
	return appender;
}

private auto appendOverride(R)(R appender, StringMap sm)
	if (isOutputRange!(R, ubyte))
{
	assert(sm.length < short.max);

	appender.append(cast(short)sm.length);
	foreach (k,v; sm) {
		appender.append(k);
		appender.append(v);
	}
	return appender;
}
private auto appendOverride(R)(R appender, Consistency c)
	if (isOutputRange!(R, ubyte))
{
	appender.append(cast(short)c);
}

private auto appendOverride(R)(R appender, bool b)
	if (isOutputRange!(R, ubyte))
{
	if (b)
		appender.append(cast(int)0x00000001);
	else
		appender.append(cast(int)0x00000000);
}
private auto appendOverride(R)(R appender, string[] strs)
	if (isOutputRange!(R, ubyte))
{
	foreach (str; strs) {
		appender.append(str);
	}
}

/*auto append(Appender!(ubyte[]) appender, ubyte[] data) {
	appender.put(data);
	return appender;
}
auto append(Appender!(ubyte[]) appender, short data) {
	import std.bitmanip : write;

	ubyte[] buffer = [0,0,0,0,0,0,0,0];
	buffer.write!short(data,0);
	appender.put(buffer[0..short.sizeof]);
	return appender;
}
auto append(Appender!(ubyte[]) appender, int data) {
	import std.bitmanip : write;

	ubyte[] buffer = [0,0,0,0,0,0,0,0];
	buffer.write!int(data,0);
	appender.put(buffer[0..int.sizeof]);
	return appender;
}
auto append(Appender!(ubyte[]) appender, string data) {
	assert(data.length < short.max);
	append(appender, cast(short)data.length);


	append(appender, cast(ubyte[])data.ptr[0..data.length]);
	return appender;
}

auto append(Appender!(ubyte[]) appender, StringMap data) {
	assert(data.length < short.max);
	append(appender, cast(short)data.length);
	foreach (k,v; data) {
		append(appender, k);
		append(appender, v);
	}

	return appender;
}*/

/**   [string multimap] A [short] n, followed by n pair <k><v> where <k> is a
 *                      [string] and <v> is a [string list].
 */
alias string[][string] StringMultiMap;
private StringMultiMap readStringMultiMap(TCPConnection s, ref int counter) {
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

class Connection {
	private {
		TCPConnection sock;
		string m_host;
		ushort m_port;
		string m_usedKeyspace;
	}

	enum defaultPort = 9042;

	this(string host, ushort port = defaultPort)
	{
		m_host = host;
		m_port = port;
	}

	void connect() {
		if (!sock || !sock.connected) {
			writeln("connecting");
			sock = connectTCP(m_host, m_port);
			writeln("connected. doing handshake...");
			startup();
			writeln("handshake completed.");
			m_usedKeyspace = null;
		}
	}

	void close() {
		if (counter > 0) {
			auto buf = readRawBytes(sock, counter, counter);
			writeln("buf:", buf);
		}
		assert(counter == 0, "Did not complete reading of stream: "~ to!string(counter) ~" bytes left");
		sock.close();
		sock = null;
	}

	void useKeyspace(string name)
	{
		if (name == m_usedKeyspace) return;
		enforceValidIdentifier(name);
		query(`USE `~name, Consistency.ANY);
		m_usedKeyspace = name;
	}



	private void write(TCPConnection s, Appender!(ubyte[]) appender) {
		//print(appender.data);
		s.write(appender.data);
		s.flush();
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
		switch (transport_version_) {
			case 1:
				fh.version_ = FrameHeader.Version.V1Request;
				break;
			case 2:
				fh.version_ = FrameHeader.Version.V2Request;
				break;
			default:
				assert(0, "invalid transport_version");
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
	 private void startup(string compression_algorithm = "") {
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

	private FrameHeader authenticate(FrameHeader fh) {
		auto authenticatorname = readAuthenticate(fh);
		auto authenticator = GetAuthenticator(authenticatorname);
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
		auto fh = makeHeader(FrameHeader.OpCode.CREDENTIALS);
		auto bytebuf = appender!(ubyte[])();
		bytebuf.append(data);
		fh.length = bytebuf.getIntLength;
		write(sock, appender!(ubyte[])().append(fh.bytes));
		write(sock, bytebuf);

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
	 *  Performs a CQL query. The body of the message consists of a CQL query as a [long
	 *  string] followed by the [consistency] for the operation.
	 *
	 *  Note that the consistency is ignored by some queries (USE, CREATE, ALTER,
	 *  TRUNCATE, ...).
	 *
	 *  The server will respond to a QUERY message with a RESULT message, the content
	 *  of which depends on the query.
	 */
	Result query(string q, Consistency consistency) {
		connect();
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
	bool insert(string q, Consistency consistency = Consistency.ANY) {
		connect();
		assert(q[0.."insert".length]=="INSERT");
		auto res = query(q, consistency);
		if (res.kind == Result.Kind.Void) {
			return true;
		}
		throw new Exception("CQLProtocolException: expected void response to insert");
	}
	Result select(string q, Consistency consistency = Consistency.QUORUM) {
		connect();
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
		connect();
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
	private auto execute(Args...)(ubyte[] preparedStatementID, Consistency consistency, Args args) {
		connect();
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
		connect();
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
	protected void throwOnError(FrameHeader fh) {
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
	 *  Indicates that the server require authentication. This will be sent following
	 *  a STARTUP message and must be answered by a CREDENTIALS message from the
	 *  client to provide authentication informations.
	 *
	 *  The body consists of a single [string] indicating the full class name of the
	 *  IAuthenticator in use.
	 */
	protected string readAuthenticate(FrameHeader fh) {
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
	protected StringMultiMap readSupported(FrameHeader fh) {
		return readStringMultiMap(sock, counter);
	}

	/**
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
		MetaData readRowMetaData(FrameHeader fh) {
			auto md = MetaData();
			md.flags.readIntNotNULL(sock, counter);
			md.columns_count.readIntNotNULL(sock, counter);
			if (md.flags & MetaData.GLOBAL_TABLES_SPEC) {
				md.global_table_spec[0] = readShortString(sock, counter);
				md.global_table_spec[1] = readShortString(sock, counter);
			}
			md.column_specs = readColumnSpecifications(md.flags & MetaData.GLOBAL_TABLES_SPEC, md.columns_count);
			writeln("got spec: ", md);
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
		Option* readOption(TCPConnection s, ref int counter) {
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
				case Option.Type.Text:
					break;
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
		protected auto readColumnSpecification(bool hasGlobalTablesSpec) {
			ColumnSpecification ret;
			if (!hasGlobalTablesSpec) {
				ret.ksname = readShortString(sock, counter);
				ret.tablename = readShortString(sock, counter);
			}
			ret.name = readShortString(sock, counter);
			ret.type = *readOption(sock, counter);
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
		protected auto readRowsContent(FrameHeader fh, MetaData md) {
			int count;
			auto tmp = readIntNotNULL(count, sock, counter);
			ReturnType!readRowContent[] ret;
			//log("reading %d rows", count);
			for (int i=0; i<count; i++) {
				ret ~= readRowContent(fh, md);
			}
			return ret;
		}
		protected auto readRowContent(FrameHeader fh, MetaData md) {
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
					case Option.Type.Text:
						goto case;
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
		protected string readSet_keyspace(FrameHeader fh) {
			assert(kind_ is Kind.Set_keyspace);
			return readShortString(sock, counter);
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
		protected void readPrepared(FrameHeader fh) {
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
		protected void readSchema_change(FrameHeader fh) {
			assert(kind_ is Kind.Schema_change);

			lastChange_ = cast(Change)readShortString(sock, counter);

			currentKeyspace_ = readShortString(sock, counter);
			currentTable_ = readShortString(sock, counter);
		}
	}

	class PreparedStatement : Result {
		ubyte[] id;
		Consistency consistency = Consistency.ANY;

		this(FrameHeader fh) {
			super(fh);
			if (kind != Result.Kind.Prepared) {
				throw new Exception("CQLProtocolException, Unknown result type for PREPARE command");
			}
		}

		/// See section 4.2.5.4.
		protected override void readPrepared(FrameHeader fh) {
			assert(kind_ is Kind.Prepared);
			id = readShortBytes(sock, counter);
			metadata = readRowMetaData(fh);
		}

		Result execute(Args...)(Args args) {
			return Connection.execute(id, consistency, args);
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
			ret ~= readBytes!T(sock, counter);
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
			ret[readShortBytes!T(sock, counter)] = readShortBytes!U(sock,counter);

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
			ret[] ~= readBytes!T(sock, counter);

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
	auto cassandra = new Connection("127.0.0.1", 9042);
	cassandra.connect();
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


private void log(Args...)(string s, Args args) {
	writefln(s, args);
}