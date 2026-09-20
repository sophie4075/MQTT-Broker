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
