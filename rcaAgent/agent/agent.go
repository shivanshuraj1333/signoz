package agent

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
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

// parseURLParams extracts parameters from a URL query string
func parseURLParams(queryString string) (map[string]string, error) {
	params := make(map[string]string)
	values, err := url.ParseQuery(queryString)
	if err != nil {
		return nil, err
	}

	for k, v := range values {
		if len(v) > 0 {
			params[k] = v[0]
		}
	}
	return params, nil
}

// extractCompositeQueryFromURL extracts the composite query from a URL
func extractCompositeQueryFromURL(urlStr string) (*CompositeQuery, error) {
	// Parse the URL
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("error parsing URL: %v", err)
	}

	// Get the query parameters
	params, err := parseURLParams(u.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("error parsing query parameters: %v", err)
	}

	// Extract the composite query
	compositeQueryStr, ok := params["compositeQuery"]
	if !ok {
		return nil, fmt.Errorf("compositeQuery parameter not found in URL")
	}

	// URL decode the composite query
	decodedQuery, err := url.QueryUnescape(compositeQueryStr)
	if err != nil {
		return nil, fmt.Errorf("error decoding composite query: %v", err)
	}

	// Parse the composite query JSON
	var compositeQuery CompositeQuery
	if err := json.Unmarshal([]byte(decodedQuery), &compositeQuery); err != nil {
		return nil, fmt.Errorf("error unmarshaling composite query: %v", err)
	}

	return &compositeQuery, nil
}

// DataSource represents the type of data source
type DataSource string

const (
	DataSourceTraces  DataSource = "traces"
	DataSourceLogs    DataSource = "logs"
	DataSourceMetrics DataSource = "metrics"
)

// QueryType represents the type of query
type QueryType string

const (
	QueryTypeUnknown       QueryType = "unknown"
	QueryTypeBuilder       QueryType = "builder"
	QueryTypeClickHouseSQL QueryType = "clickhouse_sql"
	QueryTypePromQL        QueryType = "promql"
)

// PanelType represents the type of panel
type PanelType string

const (
	PanelTypeValue PanelType = "value"
	PanelTypeGraph PanelType = "graph"
	PanelTypeTable PanelType = "table"
	PanelTypeList  PanelType = "list"
	PanelTypeTrace PanelType = "trace"
)

// CompositeQuery represents the structure of a composite query
type CompositeQuery struct {
	BuilderQueries    map[string]*BuilderQuery    `json:"builderQueries,omitempty"`
	ClickHouseQueries map[string]*ClickHouseQuery `json:"chQueries,omitempty"`
	PromQueries       map[string]*PromQuery       `json:"promQueries,omitempty"`
	PanelType         PanelType                   `json:"panelType"`
	QueryType         QueryType                   `json:"queryType"`
	Unit              string                      `json:"unit,omitempty"`
	FillGaps          bool                        `json:"fillGaps,omitempty"`
}

// BuilderQuery represents a single query in the composite query
type BuilderQuery struct {
	QueryName            string               `json:"queryName"`
	StepInterval         int64                `json:"stepInterval"`
	DataSource           DataSource           `json:"dataSource"`
	AggregateOperator    AggregateOperator    `json:"aggregateOperator"`
	AggregateAttribute   AttributeKey         `json:"aggregateAttribute,omitempty"`
	Temporality          Temporality          `json:"temporality,omitempty"`
	Filters              *FilterSet           `json:"filters,omitempty"`
	GroupBy              []AttributeKey       `json:"groupBy,omitempty"`
	Expression           string               `json:"expression"`
	Disabled             bool                 `json:"disabled"`
	Having               []Having             `json:"having,omitempty"`
	Legend               string               `json:"legend,omitempty"`
	Limit                uint64               `json:"limit"`
	Offset               uint64               `json:"offset"`
	PageSize             uint64               `json:"pageSize"`
	OrderBy              []OrderBy            `json:"orderBy,omitempty"`
	ReduceTo             ReduceToOperator     `json:"reduceTo,omitempty"`
	SelectColumns        []AttributeKey       `json:"selectColumns,omitempty"`
	TimeAggregation      TimeAggregation      `json:"timeAggregation,omitempty"`
	SpaceAggregation     SpaceAggregation     `json:"spaceAggregation,omitempty"`
	SecondaryAggregation SecondaryAggregation `json:"seriesAggregation,omitempty"`
	Functions            []Function           `json:"functions,omitempty"`
}

// ClickHouseQuery represents a ClickHouse SQL query
type ClickHouseQuery struct {
	Query    string `json:"query"`
	Disabled bool   `json:"disabled"`
	Legend   string `json:"legend,omitempty"`
}

// PromQuery represents a Prometheus query
type PromQuery struct {
	Query    string `json:"query"`
	Stats    string `json:"stats,omitempty"`
	Disabled bool   `json:"disabled"`
	Legend   string `json:"legend,omitempty"`
}

// FilterSet represents a set of filters
type FilterSet struct {
	Operator string       `json:"op,omitempty"`
	Items    []FilterItem `json:"items"`
}

// FilterItem represents a single filter
type FilterItem struct {
	Key      AttributeKey   `json:"key"`
	Value    interface{}    `json:"value"`
	Operator FilterOperator `json:"op"`
}

// AttributeKey represents an attribute key
type AttributeKey struct {
	Key      string               `json:"key"`
	DataType AttributeKeyDataType `json:"dataType"`
	Type     AttributeKeyType     `json:"type"`
	IsColumn bool                 `json:"isColumn"`
	IsJSON   bool                 `json:"isJSON"`
}

// AttributeKeyDataType represents the data type of an attribute key
type AttributeKeyDataType string

const (
	AttributeKeyDataTypeUnspecified  AttributeKeyDataType = ""
	AttributeKeyDataTypeString       AttributeKeyDataType = "string"
	AttributeKeyDataTypeInt64        AttributeKeyDataType = "int64"
	AttributeKeyDataTypeFloat64      AttributeKeyDataType = "float64"
	AttributeKeyDataTypeBool         AttributeKeyDataType = "bool"
	AttributeKeyDataTypeArrayString  AttributeKeyDataType = "array(string)"
	AttributeKeyDataTypeArrayInt64   AttributeKeyDataType = "array(int64)"
	AttributeKeyDataTypeArrayFloat64 AttributeKeyDataType = "array(float64)"
	AttributeKeyDataTypeArrayBool    AttributeKeyDataType = "array(bool)"
)

// AttributeKeyType represents the type of an attribute key
type AttributeKeyType string

const (
	AttributeKeyTypeUnspecified          AttributeKeyType = ""
	AttributeKeyTypeTag                  AttributeKeyType = "tag"
	AttributeKeyTypeResource             AttributeKeyType = "resource"
	AttributeKeyTypeInstrumentationScope AttributeKeyType = "scope"
	AttributeKeyTypeSpanSearchScope      AttributeKeyType = "spanSearchScope"
)

// Having represents a having clause
type Having struct {
	ColumnName string         `json:"columnName"`
	Operator   HavingOperator `json:"op"`
	Value      interface{}    `json:"value"`
}

// HavingOperator represents the operator in a having clause
type HavingOperator string

const (
	HavingOperatorEqual           HavingOperator = "="
	HavingOperatorNotEqual        HavingOperator = "!="
	HavingOperatorGreaterThan     HavingOperator = ">"
	HavingOperatorGreaterThanOrEq HavingOperator = ">="
	HavingOperatorLessThan        HavingOperator = "<"
	HavingOperatorLessThanOrEq    HavingOperator = "<="
	HavingOperatorIn              HavingOperator = "IN"
	HavingOperatorNotIn           HavingOperator = "NOT_IN"
)

// OrderBy represents an order by clause
type OrderBy struct {
	ColumnName string    `json:"columnName"`
	Order      Direction `json:"order"`
}

// Direction represents the direction of ordering
type Direction string

const (
	DirectionAsc  Direction = "asc"
	DirectionDesc Direction = "desc"
)

// ReduceToOperator represents the reduce to operator
type ReduceToOperator string

const (
	ReduceToOperatorLast ReduceToOperator = "last"
	ReduceToOperatorSum  ReduceToOperator = "sum"
	ReduceToOperatorAvg  ReduceToOperator = "avg"
	ReduceToOperatorMin  ReduceToOperator = "min"
	ReduceToOperatorMax  ReduceToOperator = "max"
)

// TimeAggregation represents the time aggregation type
type TimeAggregation string

const (
	TimeAggregationUnspecified   TimeAggregation = ""
	TimeAggregationAnyLast       TimeAggregation = "latest"
	TimeAggregationSum           TimeAggregation = "sum"
	TimeAggregationAvg           TimeAggregation = "avg"
	TimeAggregationMin           TimeAggregation = "min"
	TimeAggregationMax           TimeAggregation = "max"
	TimeAggregationCount         TimeAggregation = "count"
	TimeAggregationCountDistinct TimeAggregation = "count_distinct"
	TimeAggregationRate          TimeAggregation = "rate"
	TimeAggregationIncrease      TimeAggregation = "increase"
)

// SpaceAggregation represents the space aggregation type
type SpaceAggregation string

const (
	SpaceAggregationUnspecified  SpaceAggregation = ""
	SpaceAggregationSum          SpaceAggregation = "sum"
	SpaceAggregationAvg          SpaceAggregation = "avg"
	SpaceAggregationMin          SpaceAggregation = "min"
	SpaceAggregationMax          SpaceAggregation = "max"
	SpaceAggregationCount        SpaceAggregation = "count"
	SpaceAggregationPercentile50 SpaceAggregation = "p50"
	SpaceAggregationPercentile75 SpaceAggregation = "p75"
	SpaceAggregationPercentile90 SpaceAggregation = "p90"
	SpaceAggregationPercentile95 SpaceAggregation = "p95"
	SpaceAggregationPercentile99 SpaceAggregation = "p99"
)

// SecondaryAggregation represents the secondary aggregation type
type SecondaryAggregation string

const (
	SecondaryAggregationUnspecified SecondaryAggregation = ""
	SecondaryAggregationSum         SecondaryAggregation = "sum"
	SecondaryAggregationAvg         SecondaryAggregation = "avg"
	SecondaryAggregationMin         SecondaryAggregation = "min"
	SecondaryAggregationMax         SecondaryAggregation = "max"
)

// Function represents a function in a query
type Function struct {
	Name      FunctionName           `json:"name"`
	Args      []interface{}          `json:"args,omitempty"`
	NamedArgs map[string]interface{} `json:"namedArgs,omitempty"`
}

// FunctionName represents the name of a function
type FunctionName string

const (
	FunctionNameCutOffMin   FunctionName = "cutOffMin"
	FunctionNameCutOffMax   FunctionName = "cutOffMax"
	FunctionNameClampMin    FunctionName = "clampMin"
	FunctionNameClampMax    FunctionName = "clampMax"
	FunctionNameAbsolute    FunctionName = "absolute"
	FunctionNameRunningDiff FunctionName = "runningDiff"
	FunctionNameLog2        FunctionName = "log2"
	FunctionNameLog10       FunctionName = "log10"
	FunctionNameCumSum      FunctionName = "cumSum"
	FunctionNameEWMA3       FunctionName = "ewma3"
	FunctionNameEWMA5       FunctionName = "ewma5"
	FunctionNameEWMA7       FunctionName = "ewma7"
	FunctionNameMedian3     FunctionName = "median3"
	FunctionNameMedian5     FunctionName = "median5"
	FunctionNameMedian7     FunctionName = "median7"
	FunctionNameTimeShift   FunctionName = "timeShift"
	FunctionNameAnomaly     FunctionName = "anomaly"
)

// FilterOperator represents the operator in a filter
type FilterOperator string

const (
	FilterOperatorEqual           FilterOperator = "="
	FilterOperatorNotEqual        FilterOperator = "!="
	FilterOperatorGreaterThan     FilterOperator = ">"
	FilterOperatorGreaterThanOrEq FilterOperator = ">="
	FilterOperatorLessThan        FilterOperator = "<"
	FilterOperatorLessThanOrEq    FilterOperator = "<="
	FilterOperatorIn              FilterOperator = "in"
	FilterOperatorNotIn           FilterOperator = "nin"
	FilterOperatorContains        FilterOperator = "contains"
	FilterOperatorNotContains     FilterOperator = "ncontains"
	FilterOperatorRegex           FilterOperator = "regex"
	FilterOperatorNotRegex        FilterOperator = "nregex"
	FilterOperatorLike            FilterOperator = "like"
	FilterOperatorNotLike         FilterOperator = "nlike"
	FilterOperatorExists          FilterOperator = "exists"
	FilterOperatorNotExists       FilterOperator = "nexists"
	FilterOperatorHas             FilterOperator = "has"
	FilterOperatorNotHas          FilterOperator = "nhas"
)

// AggregateOperator represents the aggregate operator
type AggregateOperator string

const (
	AggregateOperatorNoOp          AggregateOperator = "noop"
	AggregateOperatorCount         AggregateOperator = "count"
	AggregateOperatorCountDistinct AggregateOperator = "count_distinct"
	AggregateOperatorSum           AggregateOperator = "sum"
	AggregateOperatorAvg           AggregateOperator = "avg"
	AggregateOperatorMin           AggregateOperator = "min"
	AggregateOperatorMax           AggregateOperator = "max"
	AggregateOperatorP05           AggregateOperator = "p05"
	AggregateOperatorP10           AggregateOperator = "p10"
	AggregateOperatorP20           AggregateOperator = "p20"
	AggregateOperatorP25           AggregateOperator = "p25"
	AggregateOperatorP50           AggregateOperator = "p50"
	AggregateOperatorP75           AggregateOperator = "p75"
	AggregateOperatorP90           AggregateOperator = "p90"
	AggregateOperatorP95           AggregateOperator = "p95"
	AggregateOperatorP99           AggregateOperator = "p99"
	AggregateOperatorRate          AggregateOperator = "rate"
	AggregateOperatorSumRate       AggregateOperator = "sum_rate"
	AggregateOperatorAvgRate       AggregateOperator = "avg_rate"
	AggregateOperatorMinRate       AggregateOperator = "min_rate"
	AggregateOperatorMaxRate       AggregateOperator = "max_rate"
	AggregateOperatorRateSum       AggregateOperator = "rate_sum"
	AggregateOperatorRateAvg       AggregateOperator = "rate_avg"
	AggregateOperatorRateMin       AggregateOperator = "rate_min"
	AggregateOperatorRateMax       AggregateOperator = "rate_max"
	AggregateOperatorHistQuant50   AggregateOperator = "hist_quantile_50"
	AggregateOperatorHistQuant75   AggregateOperator = "hist_quantile_75"
	AggregateOperatorHistQuant90   AggregateOperator = "hist_quantile_90"
	AggregateOperatorHistQuant95   AggregateOperator = "hist_quantile_95"
	AggregateOperatorHistQuant99   AggregateOperator = "hist_quantile_99"
)

// Temporality represents the temporality type
type Temporality string

const (
	Unspecified Temporality = "Unspecified"
	Delta       Temporality = "Delta"
	Cumulative  Temporality = "Cumulative"
)

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

		// Extract and process related traces/logs URLs
		if tracesURL, ok := alert.Annotations["related.traces"]; ok {
			compositeQuery, err := extractCompositeQueryFromURL(tracesURL)
			if err != nil {
				log.Printf("Error extracting composite query from traces URL: %v", err)
			} else {
				transformedAlert["composite.query.traces"] = compositeQuery
			}
		}

		if logsURL, ok := alert.Annotations["related.logs"]; ok {
			compositeQuery, err := extractCompositeQueryFromURL(logsURL)
			if err != nil {
				log.Printf("Error extracting composite query from logs URL: %v", err)
			} else {
				transformedAlert["composite.query.logs"] = compositeQuery
			}
		}

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

// DebugParseURL parses a URL and extracts the composite query
func DebugParseURL(urlStr string) error {
	// Parse the URL
	u, err := url.Parse(urlStr)
	if err != nil {
		return fmt.Errorf("error parsing URL: %v", err)
	}

	// Get query parameters
	params, err := parseURLParams(u.RawQuery)
	if err != nil {
		return fmt.Errorf("error parsing URL parameters: %v", err)
	}

	// Extract and decode composite query
	compositeQueryStr, ok := params["compositeQuery"]
	if !ok {
		return fmt.Errorf("compositeQuery parameter not found in URL")
	}

	// URL decode the composite query
	decodedQuery, err := url.QueryUnescape(compositeQueryStr)
	if err != nil {
		return fmt.Errorf("error decoding composite query: %v", err)
	}

	// Create a complete query structure
	completeQuery := struct {
		Start          int64                  `json:"start"`
		End            int64                  `json:"end"`
		Step           int64                  `json:"step"`
		Variables      map[string]interface{} `json:"variables"`
		CompositeQuery CompositeQuery         `json:"compositeQuery"`
	}{
		Start:     1739735881000, // Default start time
		End:       1742327881000, // Default end time
		Step:      8640,          // Default step
		Variables: make(map[string]interface{}),
	}

	// Create a default builder query
	defaultBuilderQuery := &BuilderQuery{
		DataSource:        "logs",
		QueryName:         "A",
		AggregateOperator: "noop",
		AggregateAttribute: AttributeKey{
			Key:      "------false",
			IsColumn: false,
		},
		TimeAggregation:  "rate",
		SpaceAggregation: "sum",
		Functions:        []Function{},
		Filters: &FilterSet{
			Items: []FilterItem{},
		},
		Expression:   "A",
		Disabled:     false,
		StepInterval: 60,
		Having:       []Having{},
		OrderBy: []OrderBy{
			{
				ColumnName: "timestamp",
				Order:      "desc",
			},
		},
		GroupBy:  []AttributeKey{},
		ReduceTo: "avg",
		Offset:   0,
		PageSize: 100,
	}

	// Set default composite query values
	completeQuery.CompositeQuery = CompositeQuery{
		QueryType: "builder",
		PanelType: "list",
		FillGaps:  false,
		BuilderQueries: map[string]*BuilderQuery{
			"A": defaultBuilderQuery,
		},
	}

	// Unmarshal the composite query from URL
	var urlCompositeQuery CompositeQuery
	if err := json.Unmarshal([]byte(decodedQuery), &urlCompositeQuery); err != nil {
		return fmt.Errorf("error unmarshaling composite query: %v", err)
	}

	// Merge URL composite query with defaults
	if urlCompositeQuery.QueryType != "" {
		completeQuery.CompositeQuery.QueryType = urlCompositeQuery.QueryType
	}
	if urlCompositeQuery.PanelType != "" {
		completeQuery.CompositeQuery.PanelType = urlCompositeQuery.PanelType
	}
	if urlCompositeQuery.FillGaps {
		completeQuery.CompositeQuery.FillGaps = true
	}
	if len(urlCompositeQuery.BuilderQueries) > 0 {
		completeQuery.CompositeQuery.BuilderQueries = urlCompositeQuery.BuilderQueries
	}

	// If time parameters are present in URL, use them
	if startStr, ok := params["start"]; ok {
		if start, err := strconv.ParseInt(startStr, 10, 64); err == nil {
			completeQuery.Start = start
		}
	}
	if endStr, ok := params["end"]; ok {
		if end, err := strconv.ParseInt(endStr, 10, 64); err == nil {
			completeQuery.End = end
		}
	}
	if stepStr, ok := params["step"]; ok {
		if step, err := strconv.ParseInt(stepStr, 10, 64); err == nil {
			completeQuery.Step = step
		}
	}

	// Marshal the complete query structure
	output, err := json.Marshal(completeQuery)
	if err != nil {
		return fmt.Errorf("error marshaling output: %v", err)
	}

	fmt.Println(string(output))
	return nil
}

// DebugParseAlert parses an alert JSON and extracts composite queries from URLs
func DebugParseAlert(alertJSON string) error {
	var alert Alert
	if err := json.Unmarshal([]byte(alertJSON), &alert); err != nil {
		return fmt.Errorf("error unmarshaling alert JSON: %v", err)
	}

	// Process traces URL if present
	if tracesURL, ok := alert.Annotations["related.traces"]; ok {
		fmt.Println("\n=== Parsing Traces URL ===")
		if err := DebugParseURL(tracesURL); err != nil {
			log.Printf("Error parsing traces URL: %v", err)
		}
	}

	// Process logs URL if present
	if logsURL, ok := alert.Annotations["related.logs"]; ok {
		fmt.Println("\n=== Parsing Logs URL ===")
		if err := DebugParseURL(logsURL); err != nil {
			log.Printf("Error parsing logs URL: %v", err)
		}
	}

	return nil
}
