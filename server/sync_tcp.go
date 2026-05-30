package server

import (
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/ris1b/jevis/config"
	"github.com/ris1b/jevis/core"
)

func RunSyncTCPServer() {
	log.Println("Starting a synchronous TCP server on", config.Host, config.Port)

	var con_clients int = 0

	lsnr, err := net.Listen("tcp", config.Host+":"+strconv.Itoa(config.Port))
	if err != nil {
		panic(err)
	}

	for {
		c, err := lsnr.Accept()
		if err != nil {
			panic(err)
		}

		con_clients += 1
		log.Println("client connected with address:", c.RemoteAddr(), "concurrent clients", con_clients)

		for {
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
			respond(cmd, c)
			// 6. Got some command from the Client: GET or PUT etc
			// log.Println("command", cmd)
			/// 7. Trigggering this response
			// if err = respond(cmd, c); err != nil {
			// 	log.Print("err write:", err)
			// }
		}
	}
}

func respond(cmd *core.RedisCmd, c net.Conn) {
	err := core.EvalAndRespond(cmd, c)
	if err != nil {
		respondError(err, c)
	}
}

func respondError(err error, c net.Conn) {
	c.Write([]byte(fmt.Sprintf("-%s\r\n", err)))
}

func readCommand(c net.Conn) (*core.RedisCmd, error) {
	var buf []byte = make([]byte, 512)

	n, err := c.Read(buf[:]) // when we read it, we put it in a buffer & we get the number of bytes read
	if err != nil {
		return nil, err
	}
	tokens, err := core.DecodeArrayString(buf[:n])
	if err != nil {
		return nil, err
	}

	// return string(buf[:n]), nil
	return &core.RedisCmd{
		Cmd:  strings.ToUpper(tokens[0]),
		Args: tokens[1:],
	}, nil
}
