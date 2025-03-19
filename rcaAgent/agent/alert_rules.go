package agent

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	v3 "go.signoz.io/signoz/pkg/query-service/model/v3"
)

// AlertRuleResponse represents the structure of an alert rule from the API
type AlertRuleResponse struct {
	Status string        `json:"status"`
	Data   AlertRuleData `json:"data"`
}

// AlertRuleData represents the data field in the alert rule response
type AlertRuleData struct {
	ID                string            `json:"id"`
	State             string            `json:"state"`
	Alert             string            `json:"alert"`
	AlertType         string            `json:"alertType"`
	RuleType          string            `json:"ruleType"`
	EvalWindow        string            `json:"evalWindow"`
	Condition         AlertCondition    `json:"condition"`
	Labels            map[string]string `json:"labels"`
	Annotations       map[string]string `json:"annotations"`
	Disabled          bool              `json:"disabled"`
	Source            string            `json:"source"`
	PreferredChannels []string          `json:"preferredChannels"`
	Version           string            `json:"version"`
	CreateAt          string            `json:"createAt"`
	CreateBy          string            `json:"createBy"`
	UpdateAt          string            `json:"updateAt"`
	UpdateBy          string            `json:"updateBy"`
}

// AlertCondition represents the condition field in the alert rule data
type AlertCondition struct {
	CompositeQuery struct {
		BuilderQueries map[string]v3.BuilderQuery `json:"builderQueries"`
		ChQueries      map[string]interface{}     `json:"chQueries"`
		PromQueries    map[string]interface{}     `json:"promQueries"`
		PanelType      string                     `json:"panelType"`
		QueryType      string                     `json:"queryType"`
	} `json:"compositeQuery"`
	Op                string `json:"op"`
	Target            int    `json:"target"`
	MatchType         string `json:"matchType"`
	Algorithm         string `json:"algorithm"`
	Seasonality       string `json:"seasonality"`
	SelectedQueryName string `json:"selectedQueryName"`
}

// AlertRuleMetric represents the structure for storing alert rule data in the database
type AlertRuleMetric struct {
	AlertID           string
	AlertName         string
	EvalWindow        string
	Types             []string
	PreferredChannels []string
	State             string
	AlertType         string
	RuleType          string
	Labels            map[string]string
	Annotations       map[string]string
	FetchedAt         time.Time
}

// FetchAlertRules fetches alert rules from the API and stores them in the database
func FetchAlertRules(db *sql.DB, apiBaseURL string, apiKey string) error {
	// Iterate through rule IDs from 0 to 100
	for ruleID := 0; ruleID <= 100; ruleID++ {
		url := fmt.Sprintf("%s/api/v1/rules/%d", apiBaseURL, ruleID)

		// Create a new request
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			log.Printf("Error creating request for rule ID %d: %v", ruleID, err)
			continue
		}

		// Add API key authentication header
		req.Header.Add("signoz-api-key", apiKey)

		// Make the HTTP request
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Error fetching rule ID %d: %v", ruleID, err)
			continue
		}

		// Skip non-200 responses
		if resp.StatusCode != http.StatusOK {
			log.Printf("Rule ID %d returned status code %d", ruleID, resp.StatusCode)
			resp.Body.Close()
			continue
		}

		// Read response body
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Printf("Error reading response for rule ID %d: %v", ruleID, err)
			continue
		}

		// Parse response
		var alertRule AlertRuleResponse
		if err := json.Unmarshal(body, &alertRule); err != nil {
			log.Printf("Error parsing response for rule ID %d: %v", ruleID, err)
			continue
		}

		// Process and store the alert rule
		if err := processAndStoreAlertRule(db, alertRule); err != nil {
			log.Printf("Error processing rule ID %d: %v", ruleID, err)
			continue
		}

		log.Printf("Successfully processed rule ID %d", ruleID)
	}

	return nil
}

// processAndStoreAlertRule processes an alert rule and stores it in the database
func processAndStoreAlertRule(db *sql.DB, response AlertRuleResponse) error {
	data := response.Data

	// Determine the alert types based on all builderQueries
	alertTypes := determineAlertTypes(data.Condition.CompositeQuery.BuilderQueries)

	// Create the metric
	metric := AlertRuleMetric{
		AlertID:           data.ID,
		AlertName:         data.Alert,
		EvalWindow:        data.EvalWindow,
		Types:             alertTypes,
		PreferredChannels: data.PreferredChannels,
		State:             data.State,
		AlertType:         data.AlertType,
		RuleType:          data.RuleType,
		Labels:            data.Labels,
		Annotations:       data.Annotations,
		FetchedAt:         time.Now(),
	}

	// Store the metric in the database
	return StoreAlertRuleMetric(db, &metric)
}

// determineAlertTypes collects types from all builder queries
func determineAlertTypes(builderQueries map[string]v3.BuilderQuery) []string {
	var types []string
	typesMap := make(map[string]bool) // To avoid duplicates

	// Iterate through all builder queries and collect their types
	for key, query := range builderQueries {

		ds := string(query.DataSource)

		dataSourceType := determineDataSourceType(ds, key)
		if !typesMap[dataSourceType] {
			typesMap[dataSourceType] = true
			types = append(types, dataSourceType)
		}
	}

	// If no types were found, return unknown
	if len(types) == 0 {
		return []string{"unknown"}
	}

	return types
}

// determineDataSourceType maps the data source to an alert type
func determineDataSourceType(dataSource, queryKey string) string {
	switch {
	case strings.EqualFold(dataSource, "logs"):
		return "logs"
	case strings.EqualFold(dataSource, "traces"):
		return "traces"
	case strings.EqualFold(dataSource, "metrics"):
		return "metrics"
	default:
		// If can't determine by data source, try to use the query key
		if queryKey == "A" {
			return "logs"
		} else if queryKey == "B" {
			return "traces"
		}
		return dataSource
	}
}

// StoreAlertRuleMetric stores the alert rule metric in the database
func StoreAlertRuleMetric(db *sql.DB, metric *AlertRuleMetric) error {
	// Convert types to a JSON string
	typesJSON, err := json.Marshal(metric.Types)
	if err != nil {
		return fmt.Errorf("error marshaling types: %v", err)
	}

	// Convert preferredChannels to a JSON string
	preferredChannelsJSON, err := json.Marshal(metric.PreferredChannels)
	if err != nil {
		return fmt.Errorf("error marshaling preferred channels: %v", err)
	}

	// Convert labels and annotations to JSON strings
	labelsJSON, err := json.Marshal(metric.Labels)
	if err != nil {
		return fmt.Errorf("error marshaling labels: %v", err)
	}

	annotationsJSON, err := json.Marshal(metric.Annotations)
	if err != nil {
		return fmt.Errorf("error marshaling annotations: %v", err)
	}

	// Insert into database
	_, err = db.Exec(`
		INSERT INTO alert_rules (
			alert_id, 
			alert_name, 
			eval_window, 
			types, 
			preferred_channels, 
			state, 
			alert_type, 
			rule_type, 
			labels, 
			annotations, 
			fetched_at
		) 
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (alert_id) 
		DO UPDATE SET 
			alert_name = EXCLUDED.alert_name,
			eval_window = EXCLUDED.eval_window,
			types = EXCLUDED.types,
			preferred_channels = EXCLUDED.preferred_channels,
			state = EXCLUDED.state,
			alert_type = EXCLUDED.alert_type,
			rule_type = EXCLUDED.rule_type,
			labels = EXCLUDED.labels,
			annotations = EXCLUDED.annotations,
			fetched_at = EXCLUDED.fetched_at
	`,
		metric.AlertID,
		metric.AlertName,
		metric.EvalWindow,
		typesJSON,
		preferredChannelsJSON,
		metric.State,
		metric.AlertType,
		metric.RuleType,
		labelsJSON,
		annotationsJSON,
		metric.FetchedAt,
	)

	return err
}
