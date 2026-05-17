package server

import (
	"io"
	"log"
	"net"
	"strconv"

	"github.com/ris1b/jevis/config"
)

func RunSyncTCPServer() {
	log.Println("Starting a synchronous TCP server on", config.Host, config.Port)

	var con_clients int = 0

	/// Actual Socket Programming startes here:
	// 1. We want our server to listen via net.Listen()
	// We want to listen on to the tcp connection at the corresponding Host and Port
	// listening to the configured host:port
	lsnr, err := net.Listen("tcp", config.Host+":"+strconv.Itoa(config.Port))
	if err != nil {
		panic(err)
	}

	// 2. When the above is executed, our server starts to listen on the particular Port
	// Any of the client can talk to the server on the Port which it is listening on

	/// 3. Once the server start, we run this infinite for loop:
	// Infinitely waiting for new connections to be accepted to my server --> i.e. any client will be able to connect to this server
	for {
		// blocking call: waiting for the new client to connect
		/// 4. For us to tell, that we are waiting for a new connection to be accepted we make this blocking call
		// lsnr is the instance of the server which we started
		// lsnr.Accepted() : Over this socket, we are accepting the connection
		// Waiting untill a client connects to this server
		c, err := lsnr.Accept()
		if err != nil {
			panic(err)
		}

		// 4. Client connects to this server

		con_clients += 1
		log.Println("client connected with address:", c.RemoteAddr(), "concurrent clients", con_clients)

		// 5. Another infinite loop for the client to continously send us commands and we (the server) would be
		// responding with the command that was sent
		/// Once we accpeted the incoming connection from the client, then this infinite loop runs which is
		/// continously reading the message over the socket
		for {
			// over the socket, continuously read the command and print it out
			/// In case there is any error (Client disconnects the connection or any issue with the socket)
			// then error won't be nil. So we want to close the scoket connection
			cmd, err := readCommand(c)
			if err != nil {
				c.Close()
				con_clients -= 1
				log.Println("client disconnected", c.RemoteAddr(), "concurrent clients", con_clients)
				if err == io.EOF {
					// graceful termination by Client
					break
				}
				log.Println("err", err)
			}
			// 6. Got some command from the Client: GET or PUT etc
			log.Println("command", cmd)
			/// 7. Trigggering this response
			if err = respond(cmd, c); err != nil {
				log.Print("err write:", err)
			}
		}
	}
}

// Accepts a command and the Socket connection
// Since we want to echo, so we are just writing it back to the socket
func respond(cmd string, c net.Conn) error {
	if _, err := c.Write([]byte(cmd)); err != nil {
		return err
	}
	return nil
}

// it takes the Socket connection and fires the system call: read
// c.Read is listening over the socket and it is trying to read message over this socket.
// So if there is nothing which is coming in from the Client, then this is a Blocking call.
// Returns the incoming message into a string
func readCommand(c net.Conn) (string, error) {
	var buf []byte = make([]byte, 512)
	n, err := c.Read(buf[:]) // when we read it, we put it in a buffer & we get the number of bytes read
	if err != nil {
		return "", err
	}
	return string(buf[:n]), nil
}
