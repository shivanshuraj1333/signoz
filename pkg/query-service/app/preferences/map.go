package preferences

var preferenceMap = map[string]Preference{
	"ORG_ONBOARDING": {
		Key:              "ORG_ONBOARDING",
		Name:             "Organisation Onboarding",
		Description:      "Organisation Onboarding",
		ValueType:        "boolean",
		DefaultValue:     false,
		AllowedValues:    []interface{}{true, false},
		IsDiscreteValues: true,
		AllowedScopes:    []string{"org"},
	},
	"THIRD_ONBOARDING": {
		Key:              "THIRD_ONBOARDING",
		Name:             "Organisation Onboarding",
		Description:      "Organisation Onboarding",
		ValueType:        "string",
		DefaultValue:     "",
		AllowedValues:    []interface{}{},
		IsDiscreteValues: true,
		AllowedScopes:    []string{"org"},
	},
}
