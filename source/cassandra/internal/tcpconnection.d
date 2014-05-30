module cassandra.internal.tcpconnection;

version(Have_vibe_d) {
	pragma(msg, "build cassandra-d with vibe");
	public import vibe.core.net : TCPConnection, connectTCP;
} else {
	pragma(msg, "build cassandra-d no vibe");
	import std.socket;

	interface TCPConnection {
		@property bool connected();
		void read(ref ubyte[] buf);
		void write(ubyte[] buf);
		void close();
		void flush();
	}
	
	class TCPConnectionImpl :TCPConnection {
		TcpSocket _socket;

		this(string host, short port) {
			_socket = new TcpSocket();
			assert(_socket, "Socket not created");
			_socket.connect(new InternetAddress(host, port));
		}

		@property
		bool connected() {
			return _socket.isAlive;
		}

		void read(ref ubyte[] buf) {
			assert(_socket, "Socket not created");
			auto n = _socket.receive(buf);
			assert(n==buf.length, "receive didn't full buffer!");
		}

		void write(ubyte[] buf) {
			assert(_socket, "Socket not created");
			auto n = _socket.send(buf);
			assert(n==buf.length, "didn't send full buffer!");
		}

		void close() {
			assert(_socket, "Socket not created");
			_socket.shutdown(SocketShutdown.BOTH);
			_socket.close();
		}

		void flush() {}
	}

	TCPConnection connectTCP(string host, short port) {
		auto ret = new TCPConnectionImpl(host, port);
		return ret;
	}
}