package main

import (
	"flag"
	"io/ioutil"
	"log"

	"github.com/SigNoz/signoz/rcaAgent/agent"
)

func main() {
	// Parse command line flags
	debug := flag.Bool("debug", false, "Enable debug mode for alert JSON parsing")
	alertFile := flag.String("alert-file", "", "File containing alert JSON to parse in debug mode")
	port := flag.String("port", ":8080", "Port to listen on")
	flag.Parse()

	if *debug {
		if *alertFile == "" {
			log.Fatal("--alert-file is required in debug mode")
		}
		// Read alert JSON from file
		alertJSON, err := ioutil.ReadFile(*alertFile)
		if err != nil {
			log.Fatalf("Error reading alert file: %v", err)
		}
		// Parse alert JSON
		if err := agent.DebugParseAlert(string(alertJSON)); err != nil {
			log.Fatalf("Error: %v", err)
		}
		return
	}

	// Start the server
	server := agent.NewServer(*port)
	if err := server.Start(); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}
