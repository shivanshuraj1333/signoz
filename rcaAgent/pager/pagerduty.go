package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const (
	apiToken  = "u+SWUkrN9GTSxHEYVnnw" // Replace with your real API key
	baseURL   = "https://api.pagerduty.com/incidents"
	dateStart = "2025-02-18T00:00:00Z"
	dateEnd   = "2025-03-18T23:59:59Z"
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

func main() {
	incidentIDs, err := fetchIncidentIDs()
	if err != nil {
		fmt.Println("Error fetching incidents:", err)
		os.Exit(1)
	}

	output := Output{
		DateRange:   fmt.Sprintf("%s to %s", dateStart, dateEnd),
		IncidentIDs: incidentIDs,
	}

	result, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		fmt.Println("Error marshalling output:", err)
		os.Exit(1)
	}

	fmt.Println(string(result))
}
