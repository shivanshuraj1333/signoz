package main

import (
	"flag"
	"log"
	"os"

	"github.com/SigNoz/signoz/rcaAgent/agent"
)

func main() {
	// Load configuration
	config, err := agent.LoadConfig()
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	// Parse command line flags
	debug := flag.Bool("debug", false, "Run in debug mode")
	alertFile := flag.String("alert-file", "", "Path to alert JSON file for debug mode")
	flag.Parse()

	if *debug {
		if *alertFile == "" {
			log.Fatal("--alert-file is required in debug mode")
		}

		// Read alert JSON file
		alertJSON, err := os.ReadFile(*alertFile)
		if err != nil {
			log.Fatalf("Error reading alert file: %v", err)
		}

		// Parse alert
		if err := agent.DebugParseAlert(alertJSON, config); err != nil {
			log.Fatalf("Error parsing alert: %v", err)
		}

		return
	}

	// Initialize database
	db, err := agent.InitDB(&config.Database)
	if err != nil {
		log.Fatalf("Error initializing database: %v", err)
	}
	defer db.Close()

	// Start server
	server := agent.NewServer(config.Server.Port, db, config)
	if err := server.Start(); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}
