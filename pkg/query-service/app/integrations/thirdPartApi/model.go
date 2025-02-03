package thirdPartApi

import v3 "go.signoz.io/signoz/pkg/query-service/model/v3"

type ThirdPartApis struct {
	Start    int64             `json:"start"`
	End      int64             `json:"end"`
	ShowIP   bool              `json:"show_ip,omitempty"`
	Domain   int64             `json:"domain,omitempty"`
	Endpoint string            `json:"endpoint,omitempty"`
	Filters  v3.FilterSet      `json:"filters,omitempty"`
	GroupBy  []v3.AttributeKey `json:"groupBy,omitempty"`
}
