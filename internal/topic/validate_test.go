package topic

import "testing"

func TestValidateFilter(t *testing.T) {
	tests := []struct {
		name        string
		filter      string
		expectError bool
	}{
		{name: "simple literal filter", filter: "a/b/c", expectError: false},
		{name: "single-level wildcard", filter: "a/+/c", expectError: false},
		{name: "multi-level wildcard", filter: "a/b/#", expectError: false},
		{name: "bash", filter: "#", expectError: false},
		{name: "empty filter", filter: "", expectError: true},
		{name: "second-level wildcard", filter: "a/#/c", expectError: true},
		{name: "wildcard mixed into a level", filter: "a+/b", expectError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateFilter(test.filter)
			gotError := err != nil
			if gotError != test.expectError {
				t.Errorf("ValidateFilter(%q) error = %v, wantErr %v", test.filter, err, test.expectError)
			}
		})
	}
}

func TestValidateMatches(t *testing.T) {
	tests := []struct {
		filter    string
		topicName string
		match     bool
	}{
		{filter: "a/b/c", topicName: "a/b/c", match: true},
		{filter: "a/b", topicName: "a/c", match: false},
		{filter: "a/+/c", topicName: "a/x/c", match: true},
		{filter: "a/+", topicName: "a/b/c", match: false},
		{filter: "a/#", topicName: "a/b/c", match: true},
		{filter: "a/#", topicName: "a", match: true},
		{filter: "+/x", topicName: "$SYS/x", match: false},
		{filter: "$SYS/+", topicName: "$SYS/uptime", match: true},
	}
	for _, test := range tests {
		t.Run(test.filter, func(t *testing.T) {
			match := Matches(test.filter, test.topicName)
			if match != test.match {
				t.Errorf("Matches(%q, %q) = %v, want %v", test.filter, test.topicName, match, test.match)
			}
		})
	}
}
