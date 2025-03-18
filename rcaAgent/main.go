package main

import (
	"log"

	"github.com/SigNoz/signoz/rcaAgent/agent"
)

func main() {
	server := agent.NewServer(":8080")
	if err := server.Start(); err != nil {
		log.Fatal(err)
	}
}
