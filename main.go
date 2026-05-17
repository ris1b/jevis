package main

import (
	"flag"
	"log"

	"github.com/ris1b/jevis/config"
	"github.com/ris1b/jevis/server"
)

func main() {
	setupFlags()
	log.Println("Jevis on a roll")

	server.RunSyncTCPServer()
}

func setupFlags() {
	flag.StringVar(&config.Host, "host", "0.0.0.0", "host for the jevis server")
	flag.IntVar(&config.Port, "port", 7379, "port for the jevis server")
	flag.Parse()
}
