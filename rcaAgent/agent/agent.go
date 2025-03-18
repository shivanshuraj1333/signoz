package agent

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
)

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
	TimeAggregationUnspecified   TimeAggregation = "Unspecified"
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
	CompositeQuery CompositeQuery         `json:"compositeQuery"`
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
	port string
}

// NewServer creates a new webhook server
func NewServer(port string) *Server {
	return &Server{port: port}
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

	// Read the request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading request body: %v", err)
		http.Error(w, "Error reading request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Parse the alert
	var alert Alert
	if err := json.Unmarshal(body, &alert); err != nil {
		log.Printf("Error unmarshaling alert: %v", err)
		http.Error(w, "Error parsing alert", http.StatusBadRequest)
		return
	}

	// Process logs URL if present
	if logsURL, ok := alert.Annotations["related.logs"]; ok {
		// Extract composite query from logs URL
		compositeQuery, err := extractCompositeQueryFromURL(logsURL)
		if err != nil {
			log.Printf("Error extracting composite query from logs URL: %v", err)
			http.Error(w, "Error extracting composite query", http.StatusBadRequest)
			return
		}

		// Create a new composite query with default values
		defaultCompositeQuery := CompositeQuery{
			QueryType: "builder",
			PanelType: "list",
			FillGaps:  false,
			BuilderQueries: map[string]*BuilderQuery{
				"A": {
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
				},
			},
		}

		// Merge the extracted query with defaults
		if compositeQuery.QueryType != "" {
			defaultCompositeQuery.QueryType = compositeQuery.QueryType
		}
		if compositeQuery.PanelType != "" {
			defaultCompositeQuery.PanelType = compositeQuery.PanelType
		}
		if compositeQuery.FillGaps {
			defaultCompositeQuery.FillGaps = true
		}
		if len(compositeQuery.BuilderQueries) > 0 {
			defaultCompositeQuery.BuilderQueries = compositeQuery.BuilderQueries
		}

		// Create query range request
		request := QueryRangeRequest{
			Start:          1739735881000, // Default start time
			End:            1742327881000, // Default end time
			Step:           8640,          // Default step
			Variables:      make(map[string]interface{}),
			CompositeQuery: defaultCompositeQuery,
		}

		// Execute the query
		apiKey := "LOPoPXo8fvlIdF7Tlm1Pal6AQnelQ/nN8A2+diGvjzk="
		queryURL := "https://nightswatch.signoz.cloud/api/v3/query_range"

		response, err := ExecuteQueryRange(queryURL, apiKey, request)
		if err != nil {
			log.Printf("Error executing query range: %v", err)
			http.Error(w, "Error executing query range", http.StatusInternalServerError)
			return
		}

		// Log the response
		log.Printf("Query Range Response: Status=%s, Error=%s, Data=%s",
			response.Status, response.Error, string(response.Data))
	}

	// Return success response
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Alert processed successfully"))
}

// DebugParseAlert parses an alert JSON and extracts composite queries from URLs
func DebugParseAlert(alertJSON string) error {
	var alert Alert
	if err := json.Unmarshal([]byte(alertJSON), &alert); err != nil {
		return fmt.Errorf("error unmarshaling alert JSON: %v", err)
	}

	// Process logs URL if present
	if logsURL, ok := alert.Annotations["related.logs"]; ok {
		fmt.Println("\n=== Parsing Logs URL ===")

		// Extract composite query from logs URL
		compositeQuery, err := extractCompositeQueryFromURL(logsURL)
		if err != nil {
			log.Printf("Error extracting composite query from logs URL: %v", err)
			return err
		}

		// Print the extracted composite query for debugging
		compositeQueryJSON, err := json.MarshalIndent(compositeQuery, "", "  ")
		if err != nil {
			log.Printf("Error marshaling composite query: %v", err)
			return err
		}
		fmt.Println("\n=== Extracted Composite Query ===")
		fmt.Println(string(compositeQueryJSON))

		// Create a new composite query with default values
		defaultCompositeQuery := CompositeQuery{
			QueryType: "builder",
			PanelType: "list",
			FillGaps:  false,
			BuilderQueries: map[string]*BuilderQuery{
				"A": {
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
				},
			},
		}

		// Merge the extracted query with defaults
		if compositeQuery.QueryType != "" {
			defaultCompositeQuery.QueryType = compositeQuery.QueryType
		}
		if compositeQuery.PanelType != "" {
			defaultCompositeQuery.PanelType = compositeQuery.PanelType
		}
		if compositeQuery.FillGaps {
			defaultCompositeQuery.FillGaps = true
		}
		if len(compositeQuery.BuilderQueries) > 0 {
			defaultCompositeQuery.BuilderQueries = compositeQuery.BuilderQueries
		}

		// Create query range request
		request := QueryRangeRequest{
			Start:          1739735881000, // Default start time
			End:            1742327881000, // Default end time
			Step:           8640,          // Default step
			Variables:      make(map[string]interface{}),
			CompositeQuery: defaultCompositeQuery,
		}

		// Print the complete request for debugging
		requestJSON, err := json.MarshalIndent(request, "", "  ")
		if err != nil {
			log.Printf("Error marshaling request: %v", err)
			return err
		}
		fmt.Println("\n=== Query Range Request ===")
		fmt.Println(string(requestJSON))

		// Execute the query
		apiKey := "LOPoPXo8fvlIdF7Tlm1Pal6AQnelQ/nN8A2+diGvjzk="
		queryURL := "https://nightswatch.signoz.cloud/api/v3/query_range"

		response, err := ExecuteQueryRange(queryURL, apiKey, request)
		if err != nil {
			log.Printf("Error executing query range: %v", err)
			return err
		}

		// Print the response
		fmt.Println("\n=== Query Range Response ===")
		fmt.Printf("Status: %s\n", response.Status)
		if response.Error != "" {
			fmt.Printf("Error: %s\n", response.Error)
		}
		if response.ErrorType != "" {
			fmt.Printf("Error Type: %s\n", response.ErrorType)
		}
		fmt.Printf("Data: %s\n", string(response.Data))
	}

	return nil
}

// ExecuteQueryRange executes a query range request
func ExecuteQueryRange(url string, apiKey string, request QueryRangeRequest) (*QueryRangeResponse, error) {
	// Marshal the request
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("error marshaling request: %v", err)
	}

	// Create the request
	req, err := http.NewRequest("POST", url, strings.NewReader(string(payload)))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	// Add headers
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("SIGNOZ-API-KEY", apiKey)

	// Send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error sending request: %v", err)
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %v", err)
	}

	// Parse the response
	var queryResponse QueryRangeResponse
	if err := json.Unmarshal(body, &queryResponse); err != nil {
		return nil, fmt.Errorf("error unmarshaling response: %v", err)
	}

	return &queryResponse, nil
}

// Helper function to parse URL parameters
func parseURLParams(query string) (map[string]string, error) {
	params := make(map[string]string)
	pairs := strings.Split(query, "&")
	for _, pair := range pairs {
		kv := strings.Split(pair, "=")
		if len(kv) != 2 {
			continue
		}
		key, err := url.QueryUnescape(kv[0])
		if err != nil {
			return nil, err
		}
		value, err := url.QueryUnescape(kv[1])
		if err != nil {
			return nil, err
		}
		params[key] = value
	}
	return params, nil
}

// Helper function to extract composite query from URL
func extractCompositeQueryFromURL(urlStr string) (*CompositeQuery, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("error parsing URL: %v", err)
	}

	params, err := parseURLParams(u.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("error parsing URL parameters: %v", err)
	}

	compositeQueryStr, ok := params["compositeQuery"]
	if !ok {
		return nil, fmt.Errorf("compositeQuery parameter not found in URL")
	}

	decodedQuery, err := url.QueryUnescape(compositeQueryStr)
	if err != nil {
		return nil, fmt.Errorf("error decoding composite query: %v", err)
	}

	var compositeQuery CompositeQuery
	if err := json.Unmarshal([]byte(decodedQuery), &compositeQuery); err != nil {
		return nil, fmt.Errorf("error unmarshaling composite query: %v", err)
	}

	return &compositeQuery, nil
}
