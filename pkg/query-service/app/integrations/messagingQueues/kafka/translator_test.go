package kafka

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v3 "go.signoz.io/signoz/pkg/query-service/model/v3"
)

func TestBuildQRParamsWithCache(t *testing.T) {
	tests := []struct {
		name           string
		messagingQueue *MessagingQueue
		queryContext   string
		attributeCache *Clients
		wantErr        bool
		expectedPanel  v3.PanelType
	}{
		{
			name: "throughput query",
			messagingQueue: &MessagingQueue{
				Start: time.Now().Add(-1 * time.Hour).UnixNano(),
				End:   time.Now().UnixNano(),
				Variables: map[string]string{
					"consumer_group": "test-group",
					"partition":      "1",
				},
			},
			queryContext:  "throughput",
			wantErr:       false,
			expectedPanel: v3.PanelTypeTable,
		},
		{
			name: "fetch-latency query",
			messagingQueue: &MessagingQueue{
				Start: time.Now().Add(-1 * time.Hour).UnixNano(),
				End:   time.Now().UnixNano(),
			},
			queryContext: "fetch-latency",
			attributeCache: &Clients{
				ServiceName:       []string{"test-service"},
				ClientID:          []string{"test-client"},
				ServiceInstanceID: []string{"test-instance"},
			},
			wantErr:       false,
			expectedPanel: v3.PanelTypeTable,
		},
		{
			name: "producer-throughput-overview query",
			messagingQueue: &MessagingQueue{
				Start: time.Now().Add(-1 * time.Hour).UnixNano(),
				End:   time.Now().UnixNano(),
			},
			queryContext:  "producer-throughput-overview",
			wantErr:       false,
			expectedPanel: v3.PanelTypeTable,
		},
		//{
		//	name: "invalid query context",
		//	messagingQueue: &MessagingQueue{
		//		Start: time.Now().Add(-1 * time.Hour).UnixNano(),
		//		End:   time.Now().UnixNano(),
		//	},
		//	queryContext: "invalid-context",
		//	wantErr:      true,
		//},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params, err := BuildQRParamsWithCache(tt.messagingQueue, tt.queryContext, tt.attributeCache)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.NotNil(t, params)
			assert.Equal(t, tt.expectedPanel, params.CompositeQuery.PanelType)
		})
	}
}

func TestBuildClickHouseQuery(t *testing.T) {
	tests := []struct {
		name           string
		messagingQueue *MessagingQueue
		queueType      string
		queryContext   string
		wantErr        bool
	}{
		{
			name: "overview query",
			messagingQueue: &MessagingQueue{
				Start: time.Now().Add(-1 * time.Hour).UnixNano(),
				End:   time.Now().UnixNano(),
			},
			queueType:    KafkaQueue,
			queryContext: "overview",
			wantErr:      false,
		},
		{
			name: "producer query",
			messagingQueue: &MessagingQueue{
				Start: time.Now().Add(-1 * time.Hour).UnixNano(),
				End:   time.Now().UnixNano(),
				Variables: map[string]string{
					"topic":     "test-topic",
					"partition": "1",
				},
			},
			queueType:    KafkaQueue,
			queryContext: "producer",
			wantErr:      false,
		},
		{
			name: "consumer query",
			messagingQueue: &MessagingQueue{
				Start: time.Now().Add(-1 * time.Hour).UnixNano(),
				End:   time.Now().UnixNano(),
				Variables: map[string]string{
					"topic":          "test-topic",
					"partition":      "1",
					"consumer_group": "test-group",
				},
			},
			queueType:    KafkaQueue,
			queryContext: "consumer",
			wantErr:      false,
		},
		{
			name: "missing required variables",
			messagingQueue: &MessagingQueue{
				Start:     time.Now().Add(-1 * time.Hour).UnixNano(),
				End:       time.Now().UnixNano(),
				Variables: map[string]string{},
			},
			queueType:    KafkaQueue,
			queryContext: "producer",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := BuildClickHouseQuery(tt.messagingQueue, tt.queueType, tt.queryContext)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.NotEmpty(t, query.Query)
		})
	}
}

func TestBuildCompositeQuery(t *testing.T) {
	tests := []struct {
		name         string
		chq          *v3.ClickHouseQuery
		queryContext string
		wantPanel    v3.PanelType
		wantErr      bool
	}{
		{
			name: "producer-consumer-eval query",
			chq: &v3.ClickHouseQuery{
				Query: "SELECT * FROM test",
			},
			queryContext: "producer-consumer-eval",
			wantPanel:    v3.PanelTypeList,
			wantErr:      false,
		},
		{
			name: "regular query",
			chq: &v3.ClickHouseQuery{
				Query: "SELECT * FROM test",
			},
			queryContext: "regular-query",
			wantPanel:    v3.PanelTypeTable,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cq, err := buildCompositeQuery(tt.chq, tt.queryContext)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.wantPanel, cq.PanelType)
			assert.NotNil(t, cq.ClickHouseQueries[tt.queryContext])
		})
	}
}

func TestBuildQueryRangeParams(t *testing.T) {
	tests := []struct {
		name           string
		messagingQueue *MessagingQueue
		queryContext   string
		wantErr        bool
	}{
		{
			name: "valid query",
			messagingQueue: &MessagingQueue{
				Start: time.Now().Add(-1 * time.Hour).UnixNano(),
				End:   time.Now().UnixNano(),
			},
			queryContext: "overview",
			wantErr:      false,
		},
		{
			name: "disabled span evaluation",
			messagingQueue: &MessagingQueue{
				Start: time.Now().Add(-1 * time.Hour).UnixNano(),
				End:   time.Now().UnixNano(),
			},
			queryContext: "producer-consumer-eval",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params, err := BuildQueryRangeParams(tt.messagingQueue, tt.queryContext)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.NotNil(t, params)
			assert.Equal(t, defaultStepInterval, params.Step)
			assert.Equal(t, "v4", params.Version)
			assert.True(t, params.FormatForWeb)
		})
	}
}
