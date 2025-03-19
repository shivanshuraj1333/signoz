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
	StartsAt     string            `json:"starts.at"`
	EndsAt       string            `json:"ends.at"`
	GeneratorURL string            `json:"generator.url"`
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

// AlertManagerAlert represents the structure of an alert from AlertManager
type AlertManagerAlert struct {
	Status       string            `json:"status"`
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	StartsAt     string            `json:"startsAt"`
	EndsAt       string            `json:"endsAt"`
	GeneratorURL string            `json:"generatorURL"`
	Fingerprint  string            `json:"fingerprint"`
}

// AlertManagerNotification represents the structure of a notification from AlertManager
type AlertManagerNotification struct {
	Version           string              `json:"version"`
	GroupKey          string              `json:"groupKey"`
	Status            string              `json:"status"`
	Receiver          string              `json:"receiver"`
	GroupLabels       map[string]string   `json:"groupLabels"`
	CommonLabels      map[string]string   `json:"commonLabels"`
	CommonAnnotations map[string]string   `json:"commonAnnotations"`
	ExternalURL       string              `json:"externalURL"`
	Alerts            []AlertManagerAlert `json:"alerts"`
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

	// Print the raw request body for debugging
	//log.Printf("Received raw alert body: %s", string(body))

	// First try to parse as a AlertManagerNotification
	var alertManagerNotification AlertManagerNotification
	if err := json.Unmarshal(body, &alertManagerNotification); err == nil && len(alertManagerNotification.Alerts) > 0 {
		log.Printf("Detected AlertManager notification format with %d alerts", len(alertManagerNotification.Alerts))

		// Process each alert in the notification
		for i, amAlert := range alertManagerNotification.Alerts {
			// Convert AlertManagerAlert to our Alert format
			alert := Alert{
				Labels:       amAlert.Labels,
				Annotations:  amAlert.Annotations,
				Status:       amAlert.Status,
				StartsAt:     amAlert.StartsAt,
				EndsAt:       amAlert.EndsAt,
				GeneratorURL: amAlert.GeneratorURL,
				Fingerprint:  amAlert.Fingerprint,
			}

			// If common labels/annotations exist, merge them
			if alertManagerNotification.CommonLabels != nil {
				if alert.Labels == nil {
					alert.Labels = make(map[string]string)
				}
				for k, v := range alertManagerNotification.CommonLabels {
					if _, exists := alert.Labels[k]; !exists {
						alert.Labels[k] = v
					}
				}
			}

			if alertManagerNotification.CommonAnnotations != nil {
				if alert.Annotations == nil {
					alert.Annotations = make(map[string]string)
				}
				for k, v := range alertManagerNotification.CommonAnnotations {
					if _, exists := alert.Annotations[k]; !exists {
						alert.Annotations[k] = v
					}
				}
			}

			log.Printf("Processing alert %d of %d from notification", i+1, len(alertManagerNotification.Alerts))
			processAlert(s, w, alert)
		}

		// Return success after processing all alerts
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("AlertManager notification processed successfully"))
		return
	}

	// If not AlertManager notification format, try our regular Alert format
	var alert Alert
	if err := json.Unmarshal(body, &alert); err != nil {
		log.Printf("Error unmarshaling alert JSON: %v", err)
		http.Error(w, "Error parsing alert JSON", http.StatusBadRequest)
		return
	}

	// Process the single alert
	processAlert(s, w, alert)
}

// processAlert handles the processing of a single alert
func processAlert(s *Server, w http.ResponseWriter, alert Alert) {
	// Log complete alert after unmarshaling
	alertJSON, _ := json.MarshalIndent(alert, "", "  ")
	log.Print("--------------------------------------------------------------------------------------")
	log.Printf("Processing alert after unmarshaling: %s\n%s", alert.Fingerprint, string(alertJSON))
	log.Print("------------------------------------------#-------------------------------------------")

	// Extract Kubernetes metadata from labels and populate properly
	kubernetesMetadata := extractKubernetesMetadata(alert.Labels)

	// Get rule_id from labels
	ruleID := alert.Labels["rule_id"]
	if ruleID == "" {
		ruleID = alert.Labels["ruleid"] // Try alternate key if the standard one is empty
	}
	if ruleID == "" {
		ruleID = alert.Labels["ruleId"] // Try camelCase variation
	}
	if ruleID == "" {
		ruleID = alert.Labels["rule.id"] // Try dot notation variation
	}

	// If not found in labels, try annotations
	if ruleID == "" && alert.Annotations != nil {
		ruleID = alert.Annotations["rule_id"]
		if ruleID == "" {
			ruleID = alert.Annotations["ruleid"]
		}
		if ruleID == "" {
			ruleID = alert.Annotations["ruleId"]
		}
		if ruleID == "" {
			ruleID = alert.Annotations["rule.id"]
		}
	}

	// Get service name from kubernetes metadata
	serviceName := ""
	if val, ok := kubernetesMetadata["service.name"]; ok {
		serviceName = val
	}

	// Initialize variables
	var queryRangeRequest QueryRangeRequest
	var response *QueryRangeResponse
	var processingTime int
	var responseK8sMetadata map[string]string
	var logsURL string = ""
	var hasLogData bool = false
	var queryErr error

	// Get logs URL from annotations if they exist
	if alert.Annotations != nil {
		logsURL = alert.Annotations["related.logs"]
	}

	// Only extract and execute query if there's a logs URL
	if logsURL != "" {
		// Extract query range request from logs URL
		queryRangeRequest, queryErr = extractCompositeQueryFromURL(logsURL)
		if queryErr != nil {
			log.Printf("Error extracting query range request for alert %s: %v", alert.Fingerprint, queryErr)
			// Continue processing even if extraction fails - we'll store the alert without log data
		} else {
			// Print the QueryRangeRequest as JSON
			//requestJSON, jsonErr := json.MarshalIndent(queryRangeRequest, "", "  ")
			//if jsonErr == nil {
			//log.Printf("QueryRangeRequest for alert %s:\n%s", alert.Fingerprint, string(requestJSON))
			//} else {
			//	log.Printf("Error marshaling QueryRangeRequest to JSON: %v", jsonErr)
			//}

			// Set step from config
			queryRangeRequest.Step = s.config.Query.Step

			// Execute query range request
			startTime := time.Now()
			response, queryErr = executeQueryRange(queryRangeRequest, s.config.SignOz)
			processingTime = int(time.Since(startTime).Milliseconds())

			if queryErr == nil && response != nil {
				hasLogData = true

				// Extract additional Kubernetes metadata from response
				responseK8sMetadata = extractKubernetesMetadataFromResponse(response)

				// Check if we can get a service name from the response
				if serviceName == "" {
					if val, ok := responseK8sMetadata["service.name"]; ok && val != "" {
						serviceName = val
					}
				}

				// Merge with the existing metadata, preferring response values if duplicates
				for key, value := range responseK8sMetadata {
					kubernetesMetadata[key] = value
				}
			}
		}
	} else {
		log.Printf("Alert %s does not contain related.logs URL, skipping query", alert.Fingerprint)
	}

	// Create alert metric
	metric := &AlertMetric{
		Timestamp:          time.Now(),
		AlertFingerprint:   alert.Fingerprint,
		AlertName:          alert.Labels["alertname"],
		AlertDescription:   alert.Annotations["description"],
		AlertSummary:       alert.Annotations["summary"],
		AlertSeverity:      alert.Labels["severity"],
		KubernetesMetadata: kubernetesMetadata,
		RuleID:             ruleID,
		Severity:           alert.Labels["severity"],
		AlertTypes:         GetAlertType(alert.Annotations),
		CompositeQuery:     nil,
		RequestJSON:        "",
		APIStatusCode:      0,
		APIResponse:        "",
		ProcessingTimeMs:   processingTime,
		ServiceName:        serviceName,
		LogBodies:          []string{},
	}

	// Update with query data if available
	if hasLogData {
		metric.CompositeQuery = &queryRangeRequest.CompositeQuery

		// Store the full query range request in RequestJSON field
		requestJSON, _ := json.MarshalIndent(queryRangeRequest, "", "  ")
		metric.RequestJSON = string(requestJSON)

		// Store the response in APIResponse field
		responseJSON, _ := json.Marshal(response)
		metric.APIResponse = string(responseJSON)

		// Set status code for successful API call
		metric.APIStatusCode = http.StatusOK

		// Extract log bodies
		metric.LogBodies = extractLogBodies(response)
	} else if logsURL != "" && queryErr != nil {
		// Store error information if query was attempted but failed
		metric.APIResponse = fmt.Sprintf("Query error: %v", queryErr)
		metric.APIStatusCode = http.StatusInternalServerError
		log.Printf("Error executing query range for alert %s: %v", alert.Fingerprint, queryErr)
	}

	// Store the metric
	if err := StoreAlertMetric(s.db, metric); err != nil {
		log.Printf("Error storing alert metric for alert %s: %v", alert.Fingerprint, err)
		http.Error(w, "Error storing alert metric", http.StatusInternalServerError)
		return
	}

	log.Printf("Alert %s processed successfully", alert.Fingerprint)
}

// DebugParseAlert parses alert JSON and prints the extracted composite query
func DebugParseAlert(alertJSON []byte, config *Config) error {
	// Print the raw alert JSON for debugging
	//fmt.Printf("Received raw alert body: %s\n", string(alertJSON))

	// First try to parse as a AlertManagerNotification
	var alertManagerNotification AlertManagerNotification
	if err := json.Unmarshal(alertJSON, &alertManagerNotification); err == nil && len(alertManagerNotification.Alerts) > 0 {
		fmt.Printf("Detected AlertManager notification format with %d alerts\n", len(alertManagerNotification.Alerts))

		// For debug mode, we only process the first alert
		if len(alertManagerNotification.Alerts) > 0 {
			amAlert := alertManagerNotification.Alerts[0]
			// Convert AlertManagerAlert to our Alert format
			alert := Alert{
				Labels:       amAlert.Labels,
				Annotations:  amAlert.Annotations,
				Status:       amAlert.Status,
				StartsAt:     amAlert.StartsAt,
				EndsAt:       amAlert.EndsAt,
				GeneratorURL: amAlert.GeneratorURL,
				Fingerprint:  amAlert.Fingerprint,
			}

			// If common labels/annotations exist, merge them
			if alertManagerNotification.CommonLabels != nil {
				if alert.Labels == nil {
					alert.Labels = make(map[string]string)
				}
				for k, v := range alertManagerNotification.CommonLabels {
					if _, exists := alert.Labels[k]; !exists {
						alert.Labels[k] = v
					}
				}
			}

			if alertManagerNotification.CommonAnnotations != nil {
				if alert.Annotations == nil {
					alert.Annotations = make(map[string]string)
				}
				for k, v := range alertManagerNotification.CommonAnnotations {
					if _, exists := alert.Annotations[k]; !exists {
						alert.Annotations[k] = v
					}
				}
			}

			return debugProcessAlert(alert, config)
		}
	}

	// If not AlertManager notification format, try our regular Alert format
	var alert Alert
	if err := json.Unmarshal(alertJSON, &alert); err != nil {
		return fmt.Errorf("error unmarshaling alert JSON: %v", err)
	}

	return debugProcessAlert(alert, config)
}

// debugProcessAlert processes a single alert in debug mode
func debugProcessAlert(alert Alert, config *Config) error {
	// Print complete alert after unmarshaling
	formattedAlert, _ := json.MarshalIndent(alert, "", "  ")
	fmt.Printf("Processing alert after unmarshaling: %s\n%s\n", alert.Fingerprint, string(formattedAlert))

	// Extract Kubernetes metadata from labels
	kubernetesMetadata := extractKubernetesMetadata(alert.Labels)

	// Get rule_id from labels
	ruleID := alert.Labels["rule_id"]
	if ruleID == "" {
		ruleID = alert.Labels["ruleid"] // Try alternate key if the standard one is empty
	}
	if ruleID == "" {
		ruleID = alert.Labels["ruleId"] // Try camelCase variation
	}
	if ruleID == "" {
		ruleID = alert.Labels["rule.id"] // Try dot notation variation
	}

	// If not found in labels, try annotations
	if ruleID == "" && alert.Annotations != nil {
		ruleID = alert.Annotations["rule_id"]
		if ruleID == "" {
			ruleID = alert.Annotations["ruleid"]
		}
		if ruleID == "" {
			ruleID = alert.Annotations["ruleId"]
		}
		if ruleID == "" {
			ruleID = alert.Annotations["rule.id"]
		}
	}

	// Get service name from kubernetes metadata
	serviceName := ""
	if val, ok := kubernetesMetadata["service.name"]; ok {
		serviceName = val
	}

	// Initialize variables
	var queryRangeRequest QueryRangeRequest
	var response *QueryRangeResponse
	var processingTime int
	var err error
	var responseK8sMetadata map[string]string
	var logsURL string = ""
	var hasLogData bool = false

	// Get logs URL from annotations if they exist
	if alert.Annotations != nil {
		logsURL = alert.Annotations["related.logs"]
	}

	// Only extract and execute query if there's a logs URL
	if logsURL != "" {
		// Extract query range request from logs URL
		queryRangeRequest, err = extractCompositeQueryFromURL(logsURL)
		if err != nil {
			fmt.Printf("Error extracting query range request for alert %s: %v\n", alert.Fingerprint, err)
			// Continue processing even if extraction fails - we'll store the alert without log data
		} else {
			// Print the QueryRangeRequest as JSON
			//requestJSON, jsonErr := json.MarshalIndent(queryRangeRequest, "", "  ")
			//if jsonErr == nil {
			//fmt.Printf("QueryRangeRequest for alert %s:\n%s\n", alert.Fingerprint, string(requestJSON))
			//} else {
			//	fmt.Printf("Error marshaling QueryRangeRequest to JSON: %v\n", jsonErr)
			//}

			// Set step from config
			queryRangeRequest.Step = config.Query.Step

			// Execute query range request
			startTime := time.Now()
			response, err = executeQueryRange(queryRangeRequest, config.SignOz)
			processingTime = int(time.Since(startTime).Milliseconds())

			if err == nil && response != nil {
				hasLogData = true

				// Extract additional Kubernetes metadata from response
				responseK8sMetadata = extractKubernetesMetadataFromResponse(response)

				// Check if we can get a service name from the response
				if serviceName == "" {
					if val, ok := responseK8sMetadata["service.name"]; ok && val != "" {
						serviceName = val
					}
				}

				// Merge with the existing metadata, preferring response values if duplicates
				for key, value := range responseK8sMetadata {
					kubernetesMetadata[key] = value
				}
			}
		}
	} else {
		fmt.Printf("Alert %s does not contain related.logs URL, skipping query\n", alert.Fingerprint)
	}

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
		KubernetesMetadata: kubernetesMetadata,
		RuleID:             ruleID,
		Severity:           alert.Labels["severity"],
		AlertTypes:         GetAlertType(alert.Annotations),
		CompositeQuery:     nil,
		RequestJSON:        "",
		APIStatusCode:      0,
		APIResponse:        "",
		ProcessingTimeMs:   processingTime,
		ServiceName:        serviceName,
		LogBodies:          []string{},
	}

	// Update with query data if available
	if hasLogData {
		metric.CompositeQuery = &queryRangeRequest.CompositeQuery

		// Store the full query range request in RequestJSON field
		requestJSON, _ := json.MarshalIndent(queryRangeRequest, "", "  ")
		metric.RequestJSON = string(requestJSON)

		// Store the response in APIResponse field
		responseJSON, _ := json.Marshal(response)
		metric.APIResponse = string(responseJSON)

		// Set status code for successful API call
		metric.APIStatusCode = http.StatusOK

		// Extract log bodies
		metric.LogBodies = extractLogBodies(response)
	} else if logsURL != "" && err != nil {
		// Store error information if query was attempted but failed
		metric.APIResponse = fmt.Sprintf("Query error: %v", err)
		metric.APIStatusCode = http.StatusInternalServerError
		fmt.Printf("Error executing query range for alert %s: %v\n", alert.Fingerprint, err)
	}

	// Store the metric
	if err := StoreAlertMetric(db, metric); err != nil {
		return fmt.Errorf("error storing alert metric for alert %s: %v", alert.Fingerprint, err)
	}

	fmt.Printf("Alert %s processed successfully\n", alert.Fingerprint)
	return nil
}

// extractCompositeQueryFromURL parses a URL and extracts a QueryRangeRequest
func extractCompositeQueryFromURL(urlToParse string) (QueryRangeRequest, error) {
	// Parse the URL to extract the query part
	parsedURL, err := url.Parse(urlToParse)
	if err != nil {
		return QueryRangeRequest{}, fmt.Errorf("error parsing URL: %v", err)
	}

	// Get the query parameters
	queryParams := parsedURL.RawQuery

	// Convert & to standard format (the URL uses \u0026 for &)
	queryParams = strings.ReplaceAll(queryParams, "\\u0026", "&")

	// Parse the URL query into QueryRangeParamsV3
	queryRangeParams, err := contextlinks.ParseLogURLToQueryParams(queryParams)
	if err != nil {
		return QueryRangeRequest{}, fmt.Errorf("error parsing query parameters: %v", err)
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

	return request, nil
}

// Helper function to parse string to int64
func parseStringToInt64(s string) (int64, error) {
	return json.Number(s).Int64()
}

// executeQueryRange executes a query range request
func executeQueryRange(request QueryRangeRequest, signOzConfig SignOzConfig) (*QueryRangeResponse, error) {
	// Convert request to JSON with pretty indentation for debugging

	requestJSON, err := json.MarshalIndent(request, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("error marshaling request: %v", err)
	}

	// Log the complete request for debugging
	//log.Printf("Executing query with request:\n%s", string(requestJSON))

	// Create HTTP request (using the same JSON for the request body)
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

	// Pretty-print the response for debugging
	//rspJSON, err := json.MarshalIndent(response, "", "  ")
	//if err == nil {
	//	log.Printf("Response:\n%s", string(rspJSON))
	//}

	return &response, nil
}

// extractKubernetesMetadata extracts Kubernetes-related metadata from labels
func extractKubernetesMetadata(labels map[string]string) map[string]string {
	metadata := make(map[string]string)

	// K8s labels with k8s. prefix (from sample response)
	k8sPrefixes := []string{
		"k8s.namespace.name",
		"k8s.container.name",
		"k8s.deployment.name",
		"k8s.statefulset.name",
		"k8s.pod.name",
		"k8s.pod.uid",
		"k8s.node.name",
		"k8s.cluster.name",
		"k8s.pod.uid",
		"k8s.node.uid",
		"k8s.node.name",
		"k8s.container.name",
		"k8s.cluster.name",
		"host.name",
		"host.id",
		"deployment.environment",
		"container.image.name",
		"container.image.tag",
		"cloud.availability_zone",
		"service.name",
	}

	// Extract from k8s prefixed labels
	for _, prefix := range k8sPrefixes {
		if value, ok := labels[prefix]; ok {
			metadata[prefix] = value
		}
	}

	// Add service.name if available
	if serviceName, ok := labels["service.name"]; ok {
		metadata["service.name"] = serviceName
	}

	return metadata
}

// extractLogBodies extracts log body contents from the query response
func extractLogBodies(response *QueryRangeResponse) []string {
	logBodies := []string{}

	// Parse the response data
	var responseData struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			QueryName string `json:"queryName"`
			List      []struct {
				Timestamp string `json:"timestamp"`
				Data      struct {
					Body             string            `json:"body"`
					ResourcesString  map[string]string `json:"resources_string"`
					AttributesString map[string]string `json:"attributes_string"`
				} `json:"data"`
			} `json:"list"`
		} `json:"result"`
	}

	if err := json.Unmarshal(response.Data, &responseData); err != nil {
		return logBodies
	}

	// Extract the log bodies
	for _, result := range responseData.Result {
		for _, item := range result.List {
			if item.Data.Body != "" {
				logBodies = append(logBodies, item.Data.Body)
			}
		}
	}

	return logBodies
}

// extractKubernetesMetadataFromResponse extracts Kubernetes-related metadata from the query response
func extractKubernetesMetadataFromResponse(response *QueryRangeResponse) map[string]string {
	metadata := make(map[string]string)

	// Parse the response data
	var responseData struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			QueryName string `json:"queryName"`
			List      []struct {
				Timestamp string `json:"timestamp"`
				Data      struct {
					ResourcesString  map[string]string `json:"resources_string"`
					AttributesString map[string]string `json:"attributes_string"`
				} `json:"data"`
			} `json:"list"`
		} `json:"result"`
	}

	if err := json.Unmarshal(response.Data, &responseData); err != nil {
		return metadata
	}

	// Important fields to extract from resources_string
	importantFields := []string{
		"service.name",
		"k8s.namespace.name",
		"k8s.deployment.name",
		"k8s.pod.name",
		"k8s.container.name",
		"k8s.cluster.name",
		"k8s.node.name",
		"k8s.statefulset.name",
		"host.name",
		"deployment.environment",
		"container.image.name",
		"container.image.tag",
	}

	// Extract the Kubernetes metadata from the first log entry
	// We assume all logs in a batch come from the same source
	for _, result := range responseData.Result {
		if len(result.List) > 0 {
			// Extract from resources_string
			if result.List[0].Data.ResourcesString != nil {
				// First, extract the important fields directly
				for _, field := range importantFields {
					if value, ok := result.List[0].Data.ResourcesString[field]; ok {
						metadata[field] = value
					}
				}

				// Then extract any other k8s related fields
				for key, value := range result.List[0].Data.ResourcesString {
					if strings.HasPrefix(key, "k8s.") && metadata[key] == "" {
						metadata[key] = value
					}
				}
			}

			// Extract from attributes_string
			if result.List[0].Data.AttributesString != nil {
				for key, value := range result.List[0].Data.AttributesString {
					// Add kubernetes related fields if present
					if strings.HasPrefix(key, "k8s.") || strings.HasPrefix(key, "kubernetes") || key == "service.name" {
						metadata[key] = value
					}
				}
			}

			// Once we've found the first log entry with data, break
			break
		}
	}

	return metadata
}
