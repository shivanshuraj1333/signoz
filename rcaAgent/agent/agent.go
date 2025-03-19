package agent

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.signoz.io/signoz/pkg/query-service/contextlinks"
	v3 "go.signoz.io/signoz/pkg/query-service/model/v3"
)

// Alert represents the structure of an alert
type Alert struct {
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	Status       string            `json:"status"`
	StartTime    string            `json:"startTime"`
	EndTime      string            `json:"endTime"`
	GeneratorURL string            `json:"generatorURL"`
	Fingerprint  string            `json:"fingerprint"`
}

// QueryRangeRequest represents the structure of a query range request
type QueryRangeRequest struct {
	Start          int64                  `json:"start"`
	End            int64                  `json:"end"`
	Step           int64                  `json:"step"`
	Variables      map[string]interface{} `json:"variables"`
	CompositeQuery v3.CompositeQuery      `json:"compositeQuery"`
}

// QueryRangeResponse represents the structure of a query range response
type QueryRangeResponse struct {
	Status    string          `json:"status"`
	Data      json.RawMessage `json:"data"`
	Error     string          `json:"error,omitempty"`
	ErrorType string          `json:"errorType,omitempty"`
}

// Server represents the webhook server
type Server struct {
	port   string
	db     *sql.DB
	config *Config
}

// NewServer creates a new webhook server
func NewServer(port int, db *sql.DB, config *Config) *Server {
	return &Server{
		port:   fmt.Sprintf(":%d", port),
		db:     db,
		config: config,
	}
}

// Start starts the webhook server
func (s *Server) Start() error {
	http.HandleFunc("/webhook", s.handleWebhook)
	log.Printf("Starting server on port %s", s.port)
	return http.ListenAndServe(s.port, nil)
}

// handleWebhook handles incoming webhook requests
func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading request body: %v", err)
		http.Error(w, "Error reading request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Parse alert JSON
	var alert Alert
	if err := json.Unmarshal(body, &alert); err != nil {
		log.Printf("Error unmarshaling alert JSON: %v", err)
		http.Error(w, "Error parsing alert JSON", http.StatusBadRequest)
		return
	}

	// Extract query range request from logs URL
	queryRangeRequest, err := extractCompositeQueryFromURL(alert.Annotations["related.logs"])
	if err != nil {
		log.Printf("Error extracting query range request: %v", err)
		http.Error(w, "Error extracting query range request", http.StatusBadRequest)
		return
	}

	// Parse start and end times from the alert
	startTime, err := time.Parse(time.RFC3339, alert.StartTime)
	if err != nil {
		log.Printf("Error parsing start time: %v", err)
		http.Error(w, "Error parsing start time", http.StatusBadRequest)
		return
	}

	endTime, err := time.Parse(time.RFC3339, alert.EndTime)
	if err != nil {
		log.Printf("Error parsing end time: %v", err)
		http.Error(w, "Error parsing end time", http.StatusBadRequest)
		return
	}

	// Update the extracted request with alert times
	queryRangeRequest.Start = startTime.UnixMilli()
	queryRangeRequest.End = endTime.UnixMilli()
	// Keep the step and variables from the parsed URL

	// Print the request for debugging
	requestJSON, err := json.MarshalIndent(queryRangeRequest, "", "  ")
	if err != nil {
		log.Printf("Error marshaling request: %v", err)
	} else {
		log.Printf("Query Range Request:\n%s", string(requestJSON))
	}

	// Execute query range request
	startTime = time.Now()
	response, err := executeQueryRange(queryRangeRequest, s.config.SignOz)
	processingTime := int(time.Since(startTime).Milliseconds())

	// Store alert metric in database with API response details
	metric := &AlertMetric{
		Timestamp:          time.Now(),
		AlertFingerprint:   alert.Fingerprint,
		AlertName:          alert.Labels["alertname"],
		AlertDescription:   alert.Annotations["description"],
		AlertSummary:       alert.Annotations["summary"],
		AlertSeverity:      alert.Labels["severity"],
		KubernetesMetadata: extractKubernetesMetadata(alert.Labels),
		RuleID:             alert.Labels["rule_id"],
		Severity:           alert.Labels["severity"],
		AlertTypes:         GetAlertType(alert.Annotations),
		CompositeQuery:     &queryRangeRequest.CompositeQuery,
		APIStatusCode:      0,
		APIResponse:        "error",
		ProcessingTimeMs:   processingTime,
	}

	if err != nil {
		log.Printf("Error executing query range: %v", err)
		metric.APIResponse = fmt.Sprintf("error: %v", err)
		http.Error(w, "Error executing query range", http.StatusInternalServerError)
	} else {
		metric.APIStatusCode = http.StatusOK
		metric.APIResponse = fmt.Sprintf("status: %s, error: %s", response.Status, response.Error)
	}

	// Store the metric
	if err := StoreAlertMetric(s.db, metric); err != nil {
		log.Printf("Error storing alert metric: %v", err)
		http.Error(w, "Error storing alert metric", http.StatusInternalServerError)
		return
	}

	// Return success response
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Alert processed successfully"))
}

// DebugParseAlert parses alert JSON and prints the extracted composite query
func DebugParseAlert(alertJSON []byte, config *Config) error {
	var alert Alert
	if err := json.Unmarshal(alertJSON, &alert); err != nil {
		return fmt.Errorf("error unmarshaling alert JSON: %v", err)
	}

	// Extract composite query from logs URL
	compositeQuery, err := extractCompositeQueryFromURL(alert.Annotations["related.logs"])
	if err != nil {
		return fmt.Errorf("error extracting composite query: %v", err)
	}

	// Create query range request with proper time range
	start, end := config.GetQueryTimeRange()
	request := QueryRangeRequest{
		Start:          start * 1000, // Convert to milliseconds
		End:            end * 1000,   // Convert to milliseconds
		Step:           config.Query.Step,
		Variables:      make(map[string]interface{}),
		CompositeQuery: compositeQuery,
	}

	// Print the extracted composite query
	fmt.Printf("Extracted Composite Query:\n")
	jsonData, err := json.MarshalIndent(compositeQuery, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling composite query: %v", err)
	}
	fmt.Println(string(jsonData))

	// Print the complete request with all details
	fmt.Printf("\nComplete Query Range Request:\n")
	fmt.Printf("Start Time: %d\n", request.Start)
	fmt.Printf("End Time: %d\n", request.End)
	fmt.Printf("Step: %d\n", request.Step)
	fmt.Printf("Variables: %v\n", request.Variables)
	fmt.Printf("Composite Query:\n")
	jsonData, err = json.MarshalIndent(request.CompositeQuery, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling composite query: %v", err)
	}
	fmt.Println(string(jsonData))

	// Print the full request JSON
	fmt.Printf("\nFull Query Range Request JSON:\n")
	jsonData, err = json.MarshalIndent(request, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling request: %v", err)
	}
	fmt.Println(string(jsonData))

	// Execute query range request
	startTime := time.Now()
	response, err := executeQueryRange(request, config.SignOz)
	processingTime := int(time.Since(startTime).Milliseconds())

	// Store alert metric in database with API response details
	db, err := InitDB(&config.Database)
	if err != nil {
		return fmt.Errorf("error initializing database: %v", err)
	}
	defer db.Close()

	// Create alert metric with API response details
	metric := &AlertMetric{
		Timestamp:          time.Now(),
		AlertFingerprint:   alert.Fingerprint,
		AlertName:          alert.Labels["alertname"],
		AlertDescription:   alert.Annotations["description"],
		AlertSummary:       alert.Annotations["summary"],
		AlertSeverity:      alert.Labels["severity"],
		KubernetesMetadata: extractKubernetesMetadata(alert.Labels),
		RuleID:             alert.Labels["rule_id"],
		Severity:           alert.Labels["severity"],
		AlertTypes:         GetAlertType(alert.Annotations),
		CompositeQuery:     &compositeQuery,
		APIStatusCode:      0,
		APIResponse:        "error",
		ProcessingTimeMs:   processingTime,
	}

	if err != nil {
		metric.APIResponse = fmt.Sprintf("error: %v", err)
		return fmt.Errorf("error executing query range: %v", err)
	} else {
		metric.APIStatusCode = http.StatusOK
		metric.APIResponse = fmt.Sprintf("status: %s, error: %s", response.Status, response.Error)
	}

	// Print the response
	fmt.Printf("\nQuery Range Response:\n")
	jsonData, err = json.MarshalIndent(response, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling response: %v", err)
	}
	fmt.Println(string(jsonData))

	// Store the metric
	if err := StoreAlertMetric(db, metric); err != nil {
		return fmt.Errorf("error storing alert metric: %v", err)
	}

	return nil
}

// extractCompositeQueryFromURL parses a URL and extracts a QueryRangeRequest
func extractCompositeQueryFromURL(urlToParse string) (QueryRangeRequest, error) {
	log.Println("Parsing URL:", urlToParse)

	// Parse the URL to extract the query part
	parsedURL, err := url.Parse(urlToParse)
	if err != nil {
		return QueryRangeRequest{}, fmt.Errorf("error parsing URL: %v", err)
	}

	// Get the query parameters
	queryParams := parsedURL.RawQuery

	// Debug output
	log.Println("Raw Query Parameters:")
	log.Println(queryParams)

	// Convert & to standard format (the URL uses \u0026 for &)
	queryParams = strings.ReplaceAll(queryParams, "\\u0026", "&")

	// Debug output after replacement
	log.Println("Query Parameters after replacement:")
	log.Println(queryParams)

	// Parse the URL query into QueryRangeParamsV3
	queryRangeParams, err := contextlinks.ParseLogURLToQueryParams(queryParams)
	if err != nil {
		return QueryRangeRequest{}, fmt.Errorf("error parsing query parameters: %v", err)
	}

	// Debug the parsed values
	log.Println("Parsed timeRange values:")
	log.Printf("  Start: %d, End: %d", queryRangeParams.Start, queryRangeParams.End)
	log.Printf("  Step: %d", queryRangeParams.Step)

	// Debug filter items
	log.Println("Parsed Filter Items:")
	for name, query := range queryRangeParams.CompositeQuery.BuilderQueries {
		log.Printf("Query: %s", name)
		if query.Filters != nil {
			log.Printf("  Operator: %s", query.Filters.Operator)
			log.Printf("  Items count: %d", len(query.Filters.Items))
			for i, filter := range query.Filters.Items {
				log.Printf("    Filter %d: %s %s %v", i+1, filter.Key.Key, filter.Operator, filter.Value)
			}
		}
	}

	// Get start and end times from URL parameters, if available
	var start, end int64
	values, _ := url.ParseQuery(queryParams)
	startTimeStr := values.Get("startTime")
	endTimeStr := values.Get("endTime")

	// If startTime and endTime are directly specified in the URL, use those values
	if startTimeStr != "" && endTimeStr != "" {
		if parsedStart, err := parseStringToInt64(startTimeStr); err == nil {
			start = parsedStart
		} else {
			start = queryRangeParams.Start
		}

		if parsedEnd, err := parseStringToInt64(endTimeStr); err == nil {
			end = parsedEnd
		} else {
			end = queryRangeParams.End
		}
	} else {
		// Otherwise use the values from the parsed query parameters
		start = queryRangeParams.Start
		end = queryRangeParams.End
	}

	// Create the QueryRangeRequest using the parsed values
	request := QueryRangeRequest{
		Start:          start,
		End:            end,
		Step:           queryRangeParams.Step,
		Variables:      queryRangeParams.Variables,
		CompositeQuery: *queryRangeParams.CompositeQuery,
	}

	// Print the formatted request for debugging
	jsonData, err := json.MarshalIndent(request, "", "  ")
	if err != nil {
		log.Printf("Error marshaling request to JSON: %v", err)
	} else {
		log.Println("Extracted QueryRangeRequest:")
		fmt.Println(string(jsonData))
	}

	return request, nil
}

// Helper function to parse string to int64
func parseStringToInt64(s string) (int64, error) {
	return json.Number(s).Int64()
}

// executeQueryRange executes a query range request
func executeQueryRange(request QueryRangeRequest, signOzConfig SignOzConfig) (*QueryRangeResponse, error) {
	// Convert request to JSON
	requestJSON, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("error marshaling request: %v", err)
	}

	// Print the request payload
	log.Printf("Sending API Request to %s:\n%s", signOzConfig.APIURL, string(requestJSON))

	// Create HTTP request
	req, err := http.NewRequest("POST", signOzConfig.APIURL, bytes.NewBuffer(requestJSON))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	if signOzConfig.APIKey != "" {
		req.Header.Set("SIGNOZ-API-KEY", signOzConfig.APIKey)
	}

	// Send request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error sending request: %v", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %v", err)
	}

	// Log response for debugging
	log.Printf("API Response:\n%s", string(body))

	// Try to parse as JSON to check if it's valid
	var jsonCheck interface{}
	if err := json.Unmarshal(body, &jsonCheck); err != nil {
		return nil, fmt.Errorf("invalid JSON response: %v, body: %s", err, string(body))
	}

	// Parse response into QueryRangeResponse
	var response QueryRangeResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("error unmarshaling response: %v, body: %s", err, string(body))
	}

	return &response, nil
}

// extractKubernetesMetadata extracts Kubernetes-related metadata from labels
func extractKubernetesMetadata(labels map[string]string) map[string]string {
	metadata := make(map[string]string)
	kubernetesPrefixes := []string{
		"kubernetes_namespace",
		"kubernetes_pod",
		"kubernetes_container",
		"kubernetes_deployment",
		"kubernetes_statefulset",
		"kubernetes_daemonset",
		"kubernetes_job",
		"kubernetes_cronjob",
	}

	for _, prefix := range kubernetesPrefixes {
		if value, ok := labels[prefix]; ok {
			metadata[prefix] = value
		}
	}

	return metadata
}
