package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

func main() {
	// Parse command line flags
	alertFile := flag.String("alert-file", "", "File containing alert JSON to send")
	webhookURL := flag.String("webhook-url", "http://localhost:8080/webhook", "Webhook URL to send the alert to")
	flag.Parse()

	if *alertFile == "" {
		log.Fatal("--alert-file is required")
	}

	// Read alert JSON from file
	alertJSON, err := os.ReadFile(*alertFile)
	if err != nil {
		log.Fatalf("Error reading alert file: %v", err)
	}

	// Send the alert to the webhook
	resp, err := http.Post(*webhookURL, "application/json", bytes.NewBuffer(alertJSON))
	if err != nil {
		log.Fatalf("Error sending alert: %v", err)
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Error reading response: %v", err)
	}

	// Print the response
	fmt.Printf("Response Status: %s\n", resp.Status)
	fmt.Printf("Response Body: %s\n", string(body))
}
