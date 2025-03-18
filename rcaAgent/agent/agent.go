package agent

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
)

// AlertmanagerWebhook represents the structure of an Alertmanager webhook payload
type AlertmanagerWebhook struct {
	Version           string            `json:"version"`
	GroupKey          string            `json:"groupKey"`
	Status            string            `json:"status"`
	Receiver          string            `json:"receiver"`
	GroupLabels       map[string]string `json:"groupLabels"`
	CommonLabels      map[string]string `json:"commonLabels"`
	CommonAnnotations map[string]string `json:"commonAnnotations"`
	ExternalURL       string            `json:"externalURL"`
	Alerts            []Alert           `json:"alerts"`
}

type Alert struct {
	Status       string            `json:"status"`
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	StartsAt     string            `json:"startsAt"`
	EndsAt       string            `json:"endsAt"`
	GeneratorURL string            `json:"generatorURL"`
	Fingerprint  string            `json:"fingerprint"`
}

// Server represents the RCA agent server
type Server struct {
	port string
}

// NewServer creates a new RCA agent server
func NewServer(port string) *Server {
	return &Server{
		port: port,
	}
}

func transformKey(key string) string {
	// Convert to lowercase and replace underscores with dots
	return strings.ToLower(strings.ReplaceAll(key, "_", "."))
}

func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Read the request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading request body: %v", err)
		http.Error(w, "Error reading request body", http.StatusBadRequest)
		return
	}
	log.Printf("Received request body: %s", string(body))

	var webhook AlertmanagerWebhook
	if err := json.Unmarshal(body, &webhook); err != nil {
		log.Printf("Error unmarshaling JSON: %v", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Process each alert
	for _, alert := range webhook.Alerts {
		// Transform data into desired format
		transformedAlert := make(map[string]interface{})

		// Transform labels
		labels := make(map[string]string)
		for k, v := range alert.Labels {
			labels[transformKey(k)] = v
		}
		transformedAlert["labels"] = labels

		// Transform annotations
		annotations := make(map[string]string)
		for k, v := range alert.Annotations {
			annotations[transformKey(k)] = v
		}
		transformedAlert["annotations"] = annotations

		// Add other fields
		transformedAlert["status"] = strings.ToLower(alert.Status)
		transformedAlert["starts.at"] = alert.StartsAt
		transformedAlert["ends.at"] = alert.EndsAt
		transformedAlert["generator.url"] = alert.GeneratorURL
		transformedAlert["fingerprint"] = alert.Fingerprint

		// Convert to JSON and print
		jsonOutput, err := json.MarshalIndent(transformedAlert, "", "  ")
		if err != nil {
			log.Printf("Error marshaling alert to JSON: %v", err)
			continue
		}
		fmt.Printf("\n=== New Alert ===\n%s\n================\n", string(jsonOutput))
	}

	w.WriteHeader(http.StatusOK)
}

// Start starts the RCA agent server
func (s *Server) Start() error {
	http.HandleFunc("/webhook", s.handleWebhook)

	fmt.Printf("Starting webhook server on port %s...\n", s.port)
	return http.ListenAndServe(s.port, nil)
}
