package cluster

import (
	"slices"
	"strings"
)

type Selector struct {
	requirements []Requirement
}

type Requirement struct {
	target selectorTarget
	key    string
	op     selectorOperator
	values []string
}

type selectorTarget uint8

const (
	selectLabel selectorTarget = iota + 1
	selectAnnotation
	selectField
)

type selectorOperator uint8

const (
	selectorEquals selectorOperator = iota + 1
	selectorExists
	selectorNotEquals
	selectorIn
	selectorNotIn
)

type SelectorTerm struct {
	target selectorTarget
	key    string
}

func Where(requirements ...Requirement) Selector {
	return Selector{requirements: append([]Requirement(nil), requirements...)}
}

func Label(key string) SelectorTerm {
	return SelectorTerm{target: selectLabel, key: key}
}

func Annotation(key string) SelectorTerm {
	return SelectorTerm{target: selectAnnotation, key: key}
}

func Field(path string) SelectorTerm {
	return SelectorTerm{target: selectField, key: path}
}

func (b SelectorTerm) Eq(value string) Requirement {
	return Requirement{target: b.target, key: b.key, op: selectorEquals, values: []string{value}}
}

func (b SelectorTerm) Exists() Requirement {
	return Requirement{target: b.target, key: b.key, op: selectorExists}
}

func (b SelectorTerm) NotEq(value string) Requirement {
	return Requirement{target: b.target, key: b.key, op: selectorNotEquals, values: []string{value}}
}

func (b SelectorTerm) In(values ...string) Requirement {
	return Requirement{target: b.target, key: b.key, op: selectorIn, values: append([]string(nil), values...)}
}

func (b SelectorTerm) NotIn(values ...string) Requirement {
	return Requirement{target: b.target, key: b.key, op: selectorNotIn, values: append([]string(nil), values...)}
}

func matchesSelector(obj Unstructured, selector Selector) bool {
	for _, requirement := range selector.requirements {
		value, exists := selectorValue(obj, requirement)
		if !requirementMatches(value, exists, requirement) {
			return false
		}
	}
	return true
}

func selectorValue(obj Unstructured, requirement Requirement) (string, bool) {
	switch requirement.target {
	case selectLabel:
		value, ok := obj.Metadata.Labels[requirement.key]
		return value, ok
	case selectAnnotation:
		value, ok := obj.Metadata.Annotations[requirement.key]
		return value, ok
	case selectField:
		return fieldStringValue(&obj, requirement.key)
	default:
		return "", false
	}
}

func requirementMatches(value string, exists bool, requirement Requirement) bool {
	switch requirement.op {
	case selectorEquals:
		return exists && len(requirement.values) == 1 && value == requirement.values[0]
	case selectorExists:
		return exists
	case selectorNotEquals:
		return !exists || len(requirement.values) == 1 && value != requirement.values[0]
	case selectorIn:
		return exists && slices.Contains(requirement.values, value)
	case selectorNotIn:
		return !exists || !slices.Contains(requirement.values, value)
	default:
		return false
	}
}

func fieldStringValue(obj *Unstructured, path string) (string, bool) {
	if strings.TrimSpace(path) == "" {
		return "", false
	}
	raw, ok := fieldRawValue(obj, path)
	if !ok {
		return "", false
	}
	return rawScalarString(raw)
}
