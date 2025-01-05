package kafka

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMessagingQueue(t *testing.T) {
	tests := []struct {
		name      string
		queue     MessagingQueue
		wantStart int64
		wantEnd   int64
		wantVars  map[string]string
	}{
		{
			name: "valid messaging queue",
			queue: MessagingQueue{
				Start: time.Now().UnixNano(),
				End:   time.Now().Add(time.Hour).UnixNano(),
				Variables: map[string]string{
					"topic":          "test-topic",
					"partition":      "1",
					"consumer_group": "test-group",
				},
			},
			wantVars: map[string]string{
				"topic":          "test-topic",
				"partition":      "1",
				"consumer_group": "test-group",
			},
		},
		{
			name: "messaging queue with eval time",
			queue: MessagingQueue{
				Start:    time.Now().UnixNano(),
				End:      time.Now().Add(time.Hour).UnixNano(),
				EvalTime: 1000000000,
				Variables: map[string]string{
					"topic": "test-topic",
				},
			},
			wantVars: map[string]string{
				"topic": "test-topic",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Greater(t, tt.queue.End, tt.queue.Start, "End time should be greater than start time")
			assert.Equal(t, tt.wantVars, tt.queue.Variables, "Variables should match")
		})
	}
}

func TestClients(t *testing.T) {
	tests := []struct {
		name    string
		clients Clients
		want    int
	}{
		{
			name: "valid clients",
			clients: Clients{
				Hash: map[string]struct{}{
					"client1": {},
					"client2": {},
				},
				ClientID:          []string{"client1", "client2"},
				ServiceInstanceID: []string{"instance1", "instance2"},
				ServiceName:       []string{"service1", "service2"},
				TopicName:         []string{"topic1", "topic2"},
			},
			want: 2,
		},
		{
			name: "empty clients",
			clients: Clients{
				Hash:              make(map[string]struct{}),
				ClientID:          []string{},
				ServiceInstanceID: []string{},
				ServiceName:       []string{},
				TopicName:         []string{},
			},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, len(tt.clients.Hash), "Hash length should match")
			assert.Equal(t, tt.want, len(tt.clients.ClientID), "ClientID length should match")
			assert.Equal(t, tt.want, len(tt.clients.ServiceInstanceID), "ServiceInstanceID length should match")
			assert.Equal(t, tt.want, len(tt.clients.ServiceName), "ServiceName length should match")
			assert.Equal(t, tt.want, len(tt.clients.TopicName), "TopicName length should match")
		})
	}
}

func TestQueueFilters(t *testing.T) {
	tests := []struct {
		name    string
		filters QueueFilters
		want    QueueFilters
	}{
		{
			name: "valid filters",
			filters: QueueFilters{
				serviceName: "test-service",
				spanName:    "test-span",
				queue:       "test-queue",
				destination: "test-destination",
				kind:        "test-kind",
			},
			want: QueueFilters{
				serviceName: "test-service",
				spanName:    "test-span",
				queue:       "test-queue",
				destination: "test-destination",
				kind:        "test-kind",
			},
		},
		{
			name: "empty filters",
			filters: QueueFilters{
				serviceName: "",
				spanName:    "",
				queue:       "",
				destination: "",
				kind:        "",
			},
			want: QueueFilters{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.filters, "Filters should match")
		})
	}
}

func TestCeleryTask(t *testing.T) {
	tests := []struct {
		name     string
		task     CeleryTask
		wantKind string
		wantType string
	}{
		{
			name: "valid celery task",
			task: CeleryTask{
				kind:   "worker",
				status: "active",
			},
			wantKind: "active",
			wantType: "active",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantKind, tt.task.GetKind(), "Kind should match")
			assert.Equal(t, tt.wantType, tt.task.GetStatus(), "Type should match")

			// Test Set method
			tt.task.Set("new-type", "new-kind")
			assert.Equal(t, "new-type", tt.task.status, "Status should be updated to new-type")
		})
	}
}

func TestCeleryTaskInterface(t *testing.T) {
	var task CeleryTasks = &CeleryTask{
		kind:   "worker",
		status: "active",
	}

	t.Run("test interface implementation", func(t *testing.T) {
		assert.Equal(t, "active", task.GetKind(), "GetKind should return status")
		assert.Equal(t, "active", task.GetStatus(), "GetType should return status")

		task.Set("new-type", "new-kind")
		assert.Equal(t, "new-type", task.GetStatus(), "Type should be updated")
		assert.Equal(t, "new-type", task.GetKind(), "Kind should be updated")
	})
}

func TestOnboardingResponse(t *testing.T) {
	tests := []struct {
		name     string
		response OnboardingResponse
		want     OnboardingResponse
	}{
		{
			name: "valid response",
			response: OnboardingResponse{
				Attribute: "test-attribute",
				Message:   "test-message",
				Status:    "success",
			},
			want: OnboardingResponse{
				Attribute: "test-attribute",
				Message:   "test-message",
				Status:    "success",
			},
		},
		{
			name: "error response",
			response: OnboardingResponse{
				Attribute: "test-attribute",
				Message:   "error occurred",
				Status:    "error",
			},
			want: OnboardingResponse{
				Attribute: "test-attribute",
				Message:   "error occurred",
				Status:    "error",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.response, "Response should match")
		})
	}
}

func TestKafkaQueueConstant(t *testing.T) {
	t.Run("kafka queue constant", func(t *testing.T) {
		assert.Equal(t, "kafka", KafkaQueue, "KafkaQueue constant should be 'kafka'")
	})
}
