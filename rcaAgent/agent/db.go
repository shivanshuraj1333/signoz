package agent

import (
	"database/sql"
	"encoding/json"
	"fmt"
	v3 "go.signoz.io/signoz/pkg/query-service/model/v3"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

// AlertType represents the type of alert based on available data sources
type AlertType string

const (
	AlertTypeLogs    AlertType = "logs"
	AlertTypeTraces  AlertType = "traces"
	AlertTypeMetrics AlertType = "metrics"
)

// AlertMetric represents the structure of alert data to be stored
type AlertMetric struct {
	Timestamp          time.Time
	AlertFingerprint   string
	AlertName          string
	AlertDescription   string
	AlertSummary       string
	AlertSeverity      string
	KubernetesMetadata map[string]string
	RuleID             string
	Severity           string
	AlertTypes         []AlertType
	CompositeQuery     *v3.CompositeQuery
	APIStatusCode      int
	APIResponse        string
	ProcessingTimeMs   int
}

// GetDSN returns the database connection string
func (c *DBConfig) GetDSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.DBName, c.SSLMode)
}

// InitDB initializes the database connection and creates tables if they don't exist
func InitDB(config *DBConfig) (*sql.DB, error) {
	db, err := sql.Open("postgres", config.GetDSN())
	if err != nil {
		return nil, fmt.Errorf("error opening database: %v", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("error connecting to database: %v", err)
	}

	// Create tables if they don't exist
	if err := createTables(db); err != nil {
		return nil, fmt.Errorf("error creating tables: %v", err)
	}

	return db, nil
}

// createTables creates the necessary database tables
func createTables(db *sql.DB) error {
	query := `
		CREATE TABLE IF NOT EXISTS alert_metrics (
			id SERIAL PRIMARY KEY,
			timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			alert_fingerprint TEXT,
			alert_name TEXT,
			alert_description TEXT,
			alert_summary TEXT,
			alert_severity TEXT,
			kubernetes_metadata JSONB,
			rule_id TEXT,
			severity TEXT,
			alert_types TEXT[],
			composite_query JSONB,
			api_status_code INTEGER,
			api_response TEXT,
			processing_time_ms INTEGER
		);

		CREATE INDEX IF NOT EXISTS idx_alert_fingerprint ON alert_metrics(alert_fingerprint);
		CREATE INDEX IF NOT EXISTS idx_alert_name ON alert_metrics(alert_name);
		CREATE INDEX IF NOT EXISTS idx_alert_types ON alert_metrics USING GIN(alert_types);
		CREATE INDEX IF NOT EXISTS idx_kubernetes_namespace ON alert_metrics((kubernetes_metadata->>'namespace'));
		CREATE INDEX IF NOT EXISTS idx_rule_id ON alert_metrics(rule_id);
		CREATE INDEX IF NOT EXISTS idx_timestamp ON alert_metrics(timestamp);
	`

	_, err := db.Exec(query)
	return err
}

// StoreAlertMetric stores an alert metric in the database
func StoreAlertMetric(db *sql.DB, metric *AlertMetric) error {
	query := `
		INSERT INTO alert_metrics (
			timestamp, alert_fingerprint, alert_name, alert_description,
			alert_summary, alert_severity, kubernetes_metadata, rule_id,
			severity, alert_types, composite_query, api_status_code,
			api_response, processing_time_ms
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
	`

	// Convert alert types to string array
	alertTypes := make([]string, len(metric.AlertTypes))
	for i, t := range metric.AlertTypes {
		alertTypes[i] = string(t)
	}

	// Convert composite query to JSON
	compositeQueryJSON, err := json.Marshal(metric.CompositeQuery)
	if err != nil {
		return fmt.Errorf("error marshaling composite query: %v", err)
	}

	// Convert kubernetes metadata to JSON
	kubernetesMetadataJSON, err := json.Marshal(metric.KubernetesMetadata)
	if err != nil {
		return fmt.Errorf("error marshaling kubernetes metadata: %v", err)
	}

	_, err = db.Exec(query,
		metric.Timestamp,
		metric.AlertFingerprint,
		metric.AlertName,
		metric.AlertDescription,
		metric.AlertSummary,
		metric.AlertSeverity,
		kubernetesMetadataJSON,
		metric.RuleID,
		metric.Severity,
		pq.Array(alertTypes),
		compositeQueryJSON,
		metric.APIStatusCode,
		metric.APIResponse,
		metric.ProcessingTimeMs,
	)

	return err
}

// GetAlertType determines the alert type based on available URLs in annotations
func GetAlertType(annotations map[string]string) []AlertType {
	var types []AlertType

	if logsURL, ok := annotations["related.logs"]; ok && logsURL != "" {
		types = append(types, AlertTypeLogs)
	}
	if tracesURL, ok := annotations["related.traces"]; ok && tracesURL != "" {
		types = append(types, AlertTypeTraces)
	}
	if metricsURL, ok := annotations["related.metrics"]; ok && metricsURL != "" {
		types = append(types, AlertTypeMetrics)
	}

	return types
}
