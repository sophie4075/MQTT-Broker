package topic

import (
	"errors"
	"strings"
)

type topicKind int

const (
	literalTopicPart topicKind = iota
	singleWildcard             // "+"
	multiWildcard              // "#"
)

func classifyTopicPart(part string) (topicKind, error) {
	switch {
	case part == "+":
		return singleWildcard, nil
	case part == "#":
		return multiWildcard, nil
	case strings.ContainsAny(part, "+#"):
		return 0, errors.New("wildcard characters '#' and '+' must occupy an entire topic level")
	default:
		return literalTopicPart, nil
	}
}

// ValidateFilter checks a Topic Filter against MQTT-4.7.1-2 and MQTT-4.7.1-3.
func ValidateFilter(filter string) error {
	if filter == "" {
		return errors.New("topic filter must be at least one character long")
	}

	topicParts := strings.Split(filter, "/")

	for i, part := range topicParts {
		kind, err := classifyTopicPart(part)
		if err != nil {
			return err
		}

		// MQTT-4.7.1-2: '#' must be the last level in the filter
		if kind == multiWildcard && i != len(topicParts)-1 {
			return errors.New("multi-level wildcard '#' must be the last level in the filter")
		}
	}

	return nil
}

// Matches reports whether topicName (a Topic Name, no wildcards) matches
// filter (a Topic Filter, may contain '+' and '#').
func Matches(filter, topicName string) bool {
	filterParts := strings.Split(filter, "/")
	topicParts := strings.Split(topicName, "/")

	// MQTT-4.7.2-1: filters starting with a wildcard never match topics
	// starting with '$'.
	if strings.HasPrefix(topicName, "$") {
		firstKind, err := classifyTopicPart(filterParts[0])
		if err != nil {
			return false
		}
		if firstKind == singleWildcard || firstKind == multiWildcard {
			return false
		}
	}

	return matchParts(filterParts, topicParts)
}

func matchParts(filterParts, topicParts []string) bool {
	for i, fp := range filterParts {
		kind, err := classifyTopicPart(fp)
		if err != nil {
			return false
		}

		switch kind {
		case multiWildcard:
			// '#' matches this level and all remaining levels (including zero).
			// Since ValidateFilter guarantees '#' is last, we can return true here.
			return true

		case singleWildcard:
			// '+' must match exactly one existing level.
			if i >= len(topicParts) {
				return false
			}

		default: // literalTopicPart
			if i >= len(topicParts) || topicParts[i] != fp {
				return false
			}
		}
	}

	// No trailing '#' consumed the rest, lengths must match exactly.
	return len(filterParts) == len(topicParts)
}
