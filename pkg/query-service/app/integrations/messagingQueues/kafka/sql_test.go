package kafka

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGenerateConsumerSQL(t *testing.T) {
	start := time.Now().Add(-1 * time.Hour).UnixNano()
	end := time.Now().UnixNano()
	tests := []struct {
		name          string
		topic         string
		partition     string
		consumerGroup string
		queueType     string
		wantContains  []string
	}{
		{
			name:          "valid consumer query",
			topic:         "test-topic",
			partition:     "1",
			consumerGroup: "test-group",
			queueType:     "kafka",
			wantContains: []string{
				"FROM signoz_traces.distributed_signoz_index_v3",
				"messaging.destination.name",
				"messaging.destination.partition.id",
				"messaging.kafka.consumer.group",
				"kind = 5",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := generateConsumerSQL(start, end, tt.topic, tt.partition, tt.consumerGroup, tt.queueType)
			for _, want := range tt.wantContains {
				assert.Contains(t, query, want)
			}
		})
	}
}

func TestGeneratePartitionLatencySQL(t *testing.T) {
	start := time.Now().Add(-1 * time.Hour).UnixNano()
	end := time.Now().UnixNano()
	tests := []struct {
		name         string
		queueType    string
		wantContains []string
	}{
		{
			name:      "valid partition latency query",
			queueType: "kafka",
			wantContains: []string{
				"WITH partition_query AS",
				"kind = 4",
				"messaging.destination.name",
				"messaging.destination.partition.id",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := generatePartitionLatencySQL(start, end, tt.queueType)
			for _, want := range tt.wantContains {
				assert.Contains(t, query, want)
			}
		})
	}
}

func TestGenerateProducerConsumerEvalSQL(t *testing.T) {
	start := time.Now().Add(-1 * time.Hour).UnixNano()
	end := time.Now().UnixNano()
	evalTime := int64(1000000000) // 1 second in nanoseconds
	tests := []struct {
		name         string
		queueType    string
		wantContains []string
	}{
		{
			name:      "valid producer consumer eval query",
			queueType: "kafka",
			wantContains: []string{
				"WITH trace_data AS",
				"INNER JOIN",
				"p.kind = 4",
				"c.kind = 5",
				"breach_percentage",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := generateProducerConsumerEvalSQL(start, end, tt.queueType, evalTime)
			for _, want := range tt.wantContains {
				assert.Contains(t, query, want)
			}
		})
	}
}

func TestGenerateNetworkLatencyThroughputSQL(t *testing.T) {
	start := time.Now().Add(-1 * time.Hour).UnixNano()
	end := time.Now().UnixNano()
	tests := []struct {
		name          string
		consumerGroup string
		partitionID   string
		queueType     string
		wantContains  []string
	}{
		{
			name:          "valid network latency query",
			consumerGroup: "test-group",
			partitionID:   "1",
			queueType:     "kafka",
			wantContains: []string{
				"messaging.client_id",
				"service.instance.id",
				"messaging.kafka.consumer.group",
				"kind = 5",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := generateNetworkLatencyThroughputSQL(start, end, tt.consumerGroup, tt.partitionID, tt.queueType)
			for _, want := range tt.wantContains {
				assert.Contains(t, query, want)
			}
		})
	}
}

func TestOnboardProducersSQL(t *testing.T) {
	start := time.Now().Add(-1 * time.Hour).UnixNano()
	end := time.Now().UnixNano()
	tests := []struct {
		name         string
		queueType    string
		wantContains []string
	}{
		{
			name:      "valid onboard producers query",
			queueType: "kafka",
			wantContains: []string{
				"COUNT(*) = 0 AS entries",
				"messaging.destination.name",
				"messaging.destination.partition.id",
				"kind = 4",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := onboardProducersSQL(start, end, tt.queueType)
			for _, want := range tt.wantContains {
				assert.Contains(t, query, want)
			}
		})
	}
}

func TestOnboardConsumerSQL(t *testing.T) {
	start := time.Now().Add(-1 * time.Hour).UnixNano()
	end := time.Now().UnixNano()
	tests := []struct {
		name         string
		queueType    string
		wantContains []string
	}{
		{
			name:      "valid onboard consumers query",
			queueType: "kafka",
			wantContains: []string{
				"COUNT(*) = 0 AS entries",
				"messaging.kafka.consumer.group",
				"messaging.message.body.size",
				"messaging.client_id",
				"service.instance.id",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := onboardConsumerSQL(start, end, tt.queueType)
			for _, want := range tt.wantContains {
				assert.Contains(t, query, want)
			}
		})
	}
}

// Helper function to test SQL query validity
func TestSQLSyntax(t *testing.T) {
	start := time.Now().Add(-1 * time.Hour).UnixNano()
	end := time.Now().UnixNano()

	tests := []struct {
		name     string
		queryFn  func() string
		required []string
	}{
		{
			name: "Consumer SQL",
			queryFn: func() string {
				return generateConsumerSQL(start, end, "test-topic", "1", "test-group", "kafka")
			},
			required: []string{"SELECT", "FROM", "WHERE", "GROUP BY"},
		},
		{
			name: "Overview SQL",
			queryFn: func() string {
				return generateOverviewSQL(start, end)
			},
			required: []string{"WITH", "SELECT", "FROM", "WHERE", "GROUP BY"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := tt.queryFn()

			// Check for basic SQL syntax elements
			for _, req := range tt.required {
				assert.True(t, strings.Contains(strings.ToUpper(query), req),
					"Query should contain %s", req)
			}

			// Check for balanced parentheses
			openCount := strings.Count(query, "(")
			closeCount := strings.Count(query, ")")
			assert.Equal(t, openCount, closeCount, "Unbalanced parentheses in query")

			// Check for proper semicolon termination
			assert.True(t, strings.TrimSpace(query)[len(strings.TrimSpace(query))-1] == ';',
				"Query should end with semicolon")
		})
	}
}
