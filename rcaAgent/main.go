package main

import (
	"flag"
	"log"
	"os"

	"go.signoz.io/signoz/rcaAgent/agent"
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
	fetchRules := flag.Bool("fetch-rules", false, "Fetch alert rules from API")
	flag.Parse()

	// Initialize database
	db, err := agent.InitDB(&config.Database)
	if err != nil {
		log.Fatalf("Error initializing database: %v", err)
	}
	defer db.Close()

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

	if *fetchRules {
		log.Println("Fetching alert rules...")
		if err := agent.FetchAlertRules(db, config.SignOz.RulesAPIBaseURL, config.SignOz.APIKey); err != nil {
			log.Fatalf("Error fetching alert rules: %v", err)
		}
		log.Println("Finished fetching alert rules")
		return
	}

	// Start server
	server := agent.NewServer(config.Server.Port, db, config)
	if err := server.Start(); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}
