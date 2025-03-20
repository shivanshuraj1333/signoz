package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

var (
	apiToken   string
	baseURL    string
	dateStart  string
	dateEnd    string
	outputFile string
	db         *sql.DB
)

type IncidentResponse struct {
	Incidents []struct {
		ID string `json:"id"`
	} `json:"incidents"`
	Offset int  `json:"offset"`
	Limit  int  `json:"limit"`
	More   bool `json:"more"`
}

type Output struct {
	DateRange   string   `json:"date_range"`
	IncidentIDs []string `json:"incident_ids"`
}

type AlertResponse struct {
	Alerts []map[string]interface{} `json:"alerts"`
	Limit  int                      `json:"limit"`
	More   bool                     `json:"more"`
	Offset int                      `json:"offset"`
	Total  int                      `json:"total"`
}

type ExtractedAlert struct {
	IncidentID  string `json:"incident_id"`
	AlertName   string `json:"alert_name"`
	RuleID      string `json:"rule_id"`
	Severity    string `json:"severity"`
	Description string `json:"description"`
	Summary     string `json:"summary"`
}

// loadConfig loads configuration from .env file
func loadConfig() error {
	// Load .env file
	err := godotenv.Load()
	if err != nil {
		return fmt.Errorf("error loading .env file: %w", err)
	}

	// Read configuration values
	apiToken = os.Getenv("PAGERDUTY_API_TOKEN")
	if apiToken == "" {
		return fmt.Errorf("PAGERDUTY_API_TOKEN is not set in .env file")
	}

	baseURL = os.Getenv("PAGERDUTY_BASE_URL")
	if baseURL == "" {
		baseURL = "https://api.pagerduty.com/incidents" // Default value
	}

	dateStart = os.Getenv("PAGERDUTY_DATE_START")
	if dateStart == "" {
		// Default to 30 days ago
		defaultStart := time.Now().AddDate(0, 0, -30).Format(time.RFC3339)
		dateStart = defaultStart
	}

	dateEnd = os.Getenv("PAGERDUTY_DATE_END")
	if dateEnd == "" {
		// Default to now
		defaultEnd := time.Now().Format(time.RFC3339)
		dateEnd = defaultEnd
	}

	outputFile = os.Getenv("PAGERDUTY_OUTPUT_FILE")
	if outputFile == "" {
		outputFile = "incident_ids.json" // Default value
	}

	return nil
}

// initDB initializes the database connection
func initDB() error {
	dbHost := os.Getenv("DB_HOST")
	if dbHost == "" {
		return fmt.Errorf("DB_HOST is not set in .env file")
	}

	dbPort := os.Getenv("DB_PORT")
	if dbPort == "" {
		dbPort = "5432" // Default PostgreSQL port
	}

	dbUser := os.Getenv("DB_USER")
	if dbUser == "" {
		return fmt.Errorf("DB_USER is not set in .env file")
	}

	dbPassword := os.Getenv("DB_PASSWORD")
	if dbPassword == "" {
		return fmt.Errorf("DB_PASSWORD is not set in .env file")
	}

	dbName := os.Getenv("DB_NAME")
	if dbName == "" {
		return fmt.Errorf("DB_NAME is not set in .env file")
	}

	dbSSLMode := os.Getenv("DB_SSL_MODE")
	if dbSSLMode == "" {
		dbSSLMode = "require" // Default to require SSL
	}

	// Construct connection string
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		dbHost, dbPort, dbUser, dbPassword, dbName, dbSSLMode)

	// Open database connection
	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}

	// Test connection
	err = db.Ping()
	if err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	fmt.Println("Successfully connected to the database")
	return nil
}

// createAlertsTable creates the pager_alerts table if it doesn't exist
func createAlertsTable() error {
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS pager_alerts (
		id SERIAL PRIMARY KEY,
		incident_id TEXT NOT NULL,
		alert_name TEXT,
		rule_id TEXT,
		severity TEXT,
		description TEXT,
		summary TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	`

	_, err := db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create pager_alerts table: %w", err)
	}

	fmt.Println("Successfully created or verified pager_alerts table")
	return nil
}

// saveAlertsToDB saves the extracted alerts to the database
func saveAlertsToDB(alerts []*ExtractedAlert) error {
	// Prepare insert statement
	stmt, err := db.Prepare(`
		INSERT INTO pager_alerts (incident_id, alert_name, rule_id, severity, description, summary)
		VALUES ($1, $2, $3, $4, $5, $6)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Insert each alert
	for _, alert := range alerts {
		_, err := tx.Stmt(stmt).Exec(
			alert.IncidentID,
			alert.AlertName,
			alert.RuleID,
			alert.Severity,
			alert.Description,
			alert.Summary,
		)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert alert: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	fmt.Printf("Successfully saved %d alerts to the database\n", len(alerts))
	return nil
}

func fetchIncidentIDs() ([]string, error) {
	client := &http.Client{Timeout: 30 * time.Second}

	var allIncidentIDs []string
	offset := 0
	limit := 100

	for {
		req, err := http.NewRequest("GET", baseURL, nil)
		if err != nil {
			return nil, err
		}

		q := req.URL.Query()
		q.Add("since", dateStart)
		q.Add("until", dateEnd)
		q.Add("limit", fmt.Sprintf("%d", limit))
		q.Add("offset", fmt.Sprintf("%d", offset))
		req.URL.RawQuery = q.Encode()

		req.Header.Add("Authorization", "Token token="+apiToken)
		req.Header.Add("Accept", "application/vnd.pagerduty+json;version=2")

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("API returned status %s: %s", resp.Status, string(body))
		}

		var incidents IncidentResponse
		if err := json.Unmarshal(body, &incidents); err != nil {
			return nil, err
		}

		for _, incident := range incidents.Incidents {
			allIncidentIDs = append(allIncidentIDs, incident.ID)
		}

		if !incidents.More {
			break
		}

		offset += limit
	}

	return allIncidentIDs, nil
}

func saveIncidentsToFile(incidentIDs []string) error {
	output := Output{
		DateRange:   fmt.Sprintf("%s to %s", dateStart, dateEnd),
		IncidentIDs: incidentIDs,
	}

	result, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshalling output: %w", err)
	}

	err = os.WriteFile(outputFile, result, 0644)
	if err != nil {
		return fmt.Errorf("error writing to file %s: %w", outputFile, err)
	}

	fmt.Printf("Saved %d incident IDs to %s\n", len(incidentIDs), outputFile)
	return nil
}

func readIncidentsFromFile() ([]string, error) {
	data, err := os.ReadFile(outputFile)
	if err != nil {
		return nil, fmt.Errorf("error reading file %s: %w", outputFile, err)
	}

	var output Output
	err = json.Unmarshal(data, &output)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling data: %w", err)
	}

	return output.IncidentIDs, nil
}

func fetchAlertsForIncident(incidentID string) (*AlertResponse, error) {
	url := fmt.Sprintf("%s/%s/alerts", baseURL, incidentID)
	method := "GET"

	client := &http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Authorization", "Token token="+apiToken)
	req.Header.Add("Accept", "application/vnd.pagerduty+json;version=2")
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status %s: %s", res.Status, string(body))
	}

	var alertResponse AlertResponse
	err = json.Unmarshal(body, &alertResponse)
	if err != nil {
		return nil, err
	}

	return &alertResponse, nil
}

func extractLabelValue(firingText, label string) string {
	re := regexp.MustCompile(fmt.Sprintf(`(?m)^\s*-\s*%s\s*=\s*(.+)$`, regexp.QuoteMeta(label)))
	matches := re.FindStringSubmatch(firingText)
	if len(matches) > 1 {
		return strings.TrimSpace(matches[1])
	}
	return ""
}

func extractAnnotationValue(firingText, annotation string) string {
	re := regexp.MustCompile(fmt.Sprintf(`(?m)^\s*-\s*%s\s*=\s*(.+)$`, regexp.QuoteMeta(annotation)))
	// Find the Annotations section
	parts := strings.Split(firingText, "Annotations:")
	if len(parts) < 2 {
		return ""
	}
	// Look for the annotation in the Annotations section
	matches := re.FindStringSubmatch(parts[1])
	if len(matches) > 1 {
		return strings.TrimSpace(matches[1])
	}
	return ""
}

func extractAlertDetails(incidentID string, alert map[string]interface{}) (*ExtractedAlert, error) {
	// Navigate through the nested structure to get the firing details
	body, ok := alert["body"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unable to parse alert body")
	}

	details, ok := body["details"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unable to parse alert details")
	}

	firing, ok := details["firing"].(string)
	if !ok {
		return nil, fmt.Errorf("unable to parse firing details")
	}

	// Extract the required fields
	alertName := extractLabelValue(firing, "alertname")
	ruleID := extractLabelValue(firing, "ruleId")
	severity := extractLabelValue(firing, "severity")
	description := extractAnnotationValue(firing, "description")
	summary := extractAnnotationValue(firing, "summary")

	return &ExtractedAlert{
		IncidentID:  incidentID,
		AlertName:   alertName,
		RuleID:      ruleID,
		Severity:    severity,
		Description: description,
		Summary:     summary,
	}, nil
}

func processAllIncidentAlerts() error {
	incidentIDs, err := readIncidentsFromFile()
	if err != nil {
		return err
	}

	fmt.Printf("Processing alerts for %d incidents\n", len(incidentIDs))

	var allExtractedAlerts []*ExtractedAlert

	maxIncidents := 0
	maxIncidentsStr := os.Getenv("PAGERDUTY_MAX_INCIDENTS")
	if maxIncidentsStr != "" {
		fmt.Sscanf(maxIncidentsStr, "%d", &maxIncidents)
	}

	for i, id := range incidentIDs {
		fmt.Printf("Fetching alerts for incident %d/%d (ID: %s)\n", i+1, len(incidentIDs), id)
		alertResponse, err := fetchAlertsForIncident(id)
		if err != nil {
			fmt.Printf("Error fetching alerts for incident %s: %v\n", id, err)
			continue
		}

		// Extract details from each alert
		for _, alert := range alertResponse.Alerts {
			extractedAlert, err := extractAlertDetails(id, alert)
			if err != nil {
				fmt.Printf("Error extracting details from alert: %v\n", err)
				continue
			}
			allExtractedAlerts = append(allExtractedAlerts, extractedAlert)
		}

		// Add a small delay to avoid hitting rate limits
		time.Sleep(500 * time.Millisecond)

		// Check if we've reached the maximum number of incidents to process
		if maxIncidents > 0 && i+1 >= maxIncidents {
			fmt.Printf("Reached maximum number of incidents to process (%d)\n", maxIncidents)
			break
		}
	}

	// Save to database
	if err := saveAlertsToDB(allExtractedAlerts); err != nil {
		return fmt.Errorf("failed to save alerts to database: %w", err)
	}

	// Get output file name from environment or use a default
	extractedAlertsFile := os.Getenv("PAGERDUTY_EXTRACTED_ALERTS_FILE")
	if extractedAlertsFile == "" {
		extractedAlertsFile = "extracted_alerts.json"
	}

	// Save all extracted alerts to a single file
	extractedData, err := json.MarshalIndent(allExtractedAlerts, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshalling extracted alerts: %w", err)
	}

	err = os.WriteFile(extractedAlertsFile, extractedData, 0644)
	if err != nil {
		return fmt.Errorf("error writing extracted alerts to file: %w", err)
	}

	fmt.Printf("Saved %d extracted alerts to %s\n", len(allExtractedAlerts), extractedAlertsFile)

	return nil
}

func main() {
	// Load configuration from .env file
	if err := loadConfig(); err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	if len(os.Args) < 2 {
		fmt.Println("Please specify a command: fetch-incidents or fetch-alerts")
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "fetch-incidents":
		incidentIDs, err := fetchIncidentIDs()
		if err != nil {
			fmt.Println("Error fetching incidents:", err)
			os.Exit(1)
		}

		err = saveIncidentsToFile(incidentIDs)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

	case "fetch-alerts":
		// Initialize database connection
		if err := initDB(); err != nil {
			log.Fatalf("Failed to initialize database: %v", err)
		}
		defer db.Close()

		// Create alerts table if it doesn't exist
		if err := createAlertsTable(); err != nil {
			log.Fatalf("Failed to create alerts table: %v", err)
		}

		err := processAllIncidentAlerts()
		if err != nil {
			fmt.Println("Error processing incident alerts:", err)
			os.Exit(1)
		}

	default:
		fmt.Println("Unknown command. Use fetch-incidents or fetch-alerts")
		os.Exit(1)
	}
}
