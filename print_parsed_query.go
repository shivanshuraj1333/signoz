package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
)

// This is a simplified version to demonstrate the concept
// Actual implementation would need proper imports from SignOz

// QueryRangeRequest represents the structure of a query range request
type QueryRangeRequest struct {
	Start          int64                  `json:"start"`
	End            int64                  `json:"end"`
	Step           int64                  `json:"step"`
	Variables      map[string]interface{} `json:"variables"`
	CompositeQuery interface{}            `json:"compositeQuery"`
}

func main() {
	// Example URL with a composite query (simplified for demonstration)
	exampleURL := "https://app.signoz.io/logs?q=&start=1678968266000&end=1678971866000&step=60&compositeQuery=%7B%22queryType%22%3A%22builder%22%2C%22builderQueries%22%3A%7B%22A%22%3A%7B%22queryName%22%3A%22A%22%2C%22dataSource%22%3A%22logs%22%2C%22filters%22%3A%7B%22items%22%3A%5B%7B%22key%22%3A%22service.name%22%2C%22op%22%3A%22%3D%22%2C%22value%22%3A%22my-service%22%7D%5D%7D%7D%7D%7D"

	fmt.Println("Parsing example URL:", exampleURL)
	fmt.Println("----------------------------------------------------")

	// Parse the URL to extract the query part
	parsedURL, err := url.Parse(exampleURL)
	if err != nil {
		fmt.Printf("Error parsing URL: %v\n", err)
		return
	}

	// Get the query parameters
	queryParams := parsedURL.RawQuery

	// Convert & to standard format (the URL uses \u0026 for &)
	queryParams = strings.ReplaceAll(queryParams, "\\u0026", "&")

	// Parse the URL query into values
	values, err := url.ParseQuery(queryParams)
	if err != nil {
		fmt.Printf("Error parsing query parameters: %v\n", err)
		return
	}

	// Extract and print the start and end times
	startTimeStr := values.Get("start")
	endTimeStr := values.Get("end")
	stepStr := values.Get("step")
	compositeQueryStr := values.Get("compositeQuery")

	// Parse start and end as int64
	var start, end, step int64
	if startTimeStr != "" {
		start, _ = parseStringToInt64(startTimeStr)
	}
	if endTimeStr != "" {
		end, _ = parseStringToInt64(endTimeStr)
	}
	if stepStr != "" {
		step, _ = parseStringToInt64(stepStr)
	}

	// Parse the composite query
	var compositeQuery interface{}
	if compositeQueryStr != "" {
		if err := json.Unmarshal([]byte(compositeQueryStr), &compositeQuery); err != nil {
			fmt.Printf("Error parsing composite query: %v\n", err)
		}
	}

	// Create the QueryRangeRequest
	request := QueryRangeRequest{
		Start:          start,
		End:            end,
		Step:           step,
		Variables:      make(map[string]interface{}),
		CompositeQuery: compositeQuery,
	}

	// Print the QueryRangeRequest
	fmt.Println("QueryRangeRequest:")
	fmt.Printf("  Start: %d\n", request.Start)
	fmt.Printf("  End: %d\n", request.End)
	fmt.Printf("  Step: %d\n", request.Step)

	// Print the composite query
	fmt.Println("\nCompositeQuery:")
	compositeQueryJSON, _ := json.MarshalIndent(request.CompositeQuery, "", "  ")
	fmt.Println(string(compositeQueryJSON))
}

// Helper function to parse string to int64
func parseStringToInt64(s string) (int64, error) {
	var n json.Number = json.Number(s)
	return n.Int64()
}
