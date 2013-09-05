import std.socket;

version(Have_vibe_d) {}
else {
	interface TCPConnection {
		void read(ref ubyte[] buf);
		void write(ubyte[] buf, bool flush = false);
		void close();
	}
	class TCPConnectionImpl :TCPConnection {
		TcpSocket _socket;

		this(string host, short port) {
			_socket = new TcpSocket();
			assert(_socket, "Socket not created");
			_socket.connect(new InternetAddress(host, port));
		}
		void read(ref ubyte[] buf) {
			assert(_socket, "Socket not created");
			auto n = _socket.receive(buf);
			assert(n==buf.length, "receive didn't full buffer!");
		}
		void write(ubyte[] buf, bool flushafter = false) {
			assert(_socket, "Socket not created");
			auto n = _socket.send(buf);
			assert(n==buf.length, "didn't send full buffer!");
		}

		void close() {
			assert(_socket, "Socket not created");
			_socket.shutdown(SocketShutdown.BOTH);
			_socket.close();
		}
	}
	alias TCPConnection TcpConnection;

	TCPConnection connectTcp(string host, short port) {
		auto ret = new TCPConnectionImpl(host, port);
		return ret;
	}
}