package cluster

import "strings"

type Selector struct {
	requirements []Requirement
}

type Requirement struct {
	target selectorTarget
	key    string
	value  string
}

type selectorTarget uint8

const (
	selectLabel selectorTarget = iota + 1
	selectAnnotation
	selectField
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
	return Requirement{target: b.target, key: b.key, value: value}
}

func matchesSelector(obj Unstructured, selector Selector) bool {
	for _, requirement := range selector.requirements {
		switch requirement.target {
		case selectLabel:
			if obj.Metadata.Labels[requirement.key] != requirement.value {
				return false
			}
		case selectAnnotation:
			if obj.Metadata.Annotations[requirement.key] != requirement.value {
				return false
			}
		case selectField:
			value, ok := fieldStringValue(&obj, requirement.key)
			if !ok || value != requirement.value {
				return false
			}
		default:
			return false
		}
	}
	return true
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
