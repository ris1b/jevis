package server

import (
	"log"
	"net"
	"syscall"

	"github.com/ris1b/jevis/config"
	"github.com/ris1b/jevis/core"
)

var con_clients int = 0

// Socket Stream: typically tells that it doesn't want to disconnect it's tcp connection as soon as it got a reply. It wants
// to keep the tcp connection open as it is a streaming connection

func RunAsyncTCPServer() error {
	log.Println("starting an asynchronous tcp server on", config.Host, config.Port)

	max_clients := 20000

	// Create EPOLL event Objects to hold events
	// We have to register our File Descriptors in EPOLL, when some FD is ready for IO, then we would be getting it in this buffer called events
	// so they hold the file descriptors which are ready for an io by epoll system call
	var events []syscall.EpollEvent = make([]syscall.EpollEvent, max_clients)
	log.Println("1")
	// We want access to raw file descriptors,
	// create a socket --> an IPV4 socket, which is non-blocking and a socket stream
	serverFD, err := syscall.Socket(syscall.AF_INET, syscall.O_NONBLOCK|syscall.SOCK_STREAM, 0)

	if err != nil {
		return err
	}
	defer syscall.Close(serverFD)

	// Set the socket operate in a non-blocking mode
	if err = syscall.SetNonblock(serverFD, true); err != nil {
		return err
	}

	// Building the ip and the port
	ip4 := net.ParseIP(config.Host)
	if err = syscall.Bind(serverFD, &syscall.SockaddrInet4{
		Port: config.Port,
		Addr: [4]byte{ip4[0], ip4[1], ip4[2], ip4[3]},
	}); err != nil {
		return err
	}

	// Start listening
	if err = syscall.Listen(serverFD, max_clients); err != nil {
		return err
	}

	// AsyncIO !
	// creating EPOLL instance
	epollFD, err := syscall.EpollCreate1(0)
	if err != nil {
		log.Fatal(err)
	}

	defer syscall.Close(epollFD)

	// configuring the events for which we want to get notified along with the socket fd
	var socketServerEvent syscall.EpollEvent = syscall.EpollEvent{
		Events: syscall.EPOLLIN,
		Fd:     int32(serverFD),
	}

	// listen to read events on the server itself
	if err = syscall.EpollCtl(epollFD, syscall.EPOLL_CTL_ADD, serverFD, &socketServerEvent); err != nil {
		return err
	}

	for {
		log.Println("inside infinite for loop")
		// waiting for any FD ready for the IO
		nevents, e := syscall.EpollWait(epollFD, events[:], -1)
		if e != nil {
			continue
		}

		for i := 0; i < nevents; i++ {
			log.Println("inside nested for loop")
			if int(events[i].Fd) == serverFD {
				fd, _, err := syscall.Accept(serverFD)
				if err != nil {
					log.Println("err", err)
					continue
				}

				// increase the number of concurrent clients count
				con_clients++
				syscall.SetNonblock(serverFD, true)

				// monitor this
				var socketClientEvent syscall.EpollEvent = syscall.EpollEvent{
					Events: syscall.EPOLLIN,
					Fd:     int32(fd),
				}
				if err := syscall.EpollCtl(epollFD, syscall.EPOLL_CTL_ADD, fd, &socketClientEvent); err != nil {
					log.Fatal(err)
				}
			} else {
				comm := core.FDComm{Fd: int(events[i].Fd)}
				cmd, err := readCommand(comm)
				if err != nil {
					syscall.Close(int(events[i].Fd))
					con_clients -= 1
					continue
				}
				respond(cmd, comm)
			}
		}
	}

}
