module cassandra.internal.utils;

import std.array : appender;
import std.format : formattedWrite;

string hex(T)(T v) {
	auto buf = appender!string();
	ubyte* ptr = cast(ubyte*)&v;
	foreach (b; ptr[0..v.sizeof]) {
		formattedWrite(buf, "%x ", b);
	}
	return buf.data;
}

void enforceValidIdentifier(string text)
{
	foreach (ch; text) {
		switch (ch) {
			default: throw new Exception("Invalid identifier, '"~text~"'.");
			case 'a': .. case 'z':
			case 'A': .. case 'Z':
			case '0': .. case '9':
			case '_':
				break;
		}
	}
}

/*void print(ubyte[] data) {
	import std.ascii;
	foreach (d1; data) {
		auto d = cast(char)d1;
		if (isPrintable(d)) {
			writef("%c ", d);
		} else {
			writef("%2x ", d);
		}
	}
	writeln();
}
*/



/*ubyte[] toBytes(string[string] sm) {
	return null;
}
ubyte[] toBytes(int i) {
	return null;
}
/*void write(Socket s, StringMap data) {
	assert(data.length < short.max);
	write(s, cast(short)data.length);
	foreach (k,v; data) {
		writeln("kv:", k, v);
		write(s, k);
		write(s, v);
	}
	writeln("wrote StringMap");
}

/// This implementation even seems to use the compilers implicit casts
bool hasUFCSmember(T, string member)() {
	T v;
	return __traits(compiles, mixin("v."~ member));
}*/