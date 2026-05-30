package cluster

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
)

type ResourceDef[S, T any] struct {
	Resource   string
	APIVersion string
	Kind       string

	Annotations []AnnotationRule
	Default     func(*Object[S, T]) error
	Validate    func(oldObj, newObj *Object[S, T], subresource Subresource) error
}

type AnnotationRule struct {
	Key       string
	Required  bool
	Immutable bool
	Indexed   bool
	Watch     bool
	Default   string
}

type resourceDefinition struct {
	Resource   string
	APIVersion string
	Kind       string

	annotationRules map[string]AnnotationRule
	specRules       []fieldRule
	statusRules     []fieldRule
	defaultObject   func(*Unstructured) error
	validateObject  func(oldObj, newObj *Unstructured, subresource Subresource) error
}

type fieldRule struct {
	Path      string
	Required  bool
	Immutable bool
	Indexed   bool
	Watch     bool
	Enum      []string
	IndexName string
}

func Define[S, T any](c *Cluster, def ResourceDef[S, T]) (*Resource[S, T], error) {
	if c == nil {
		return nil, ErrInvalidConfig
	}
	rawDef, err := buildDefinition(def)
	if err != nil {
		return nil, err
	}
	if err := c.registerDefinition(rawDef); err != nil {
		return nil, err
	}
	return &Resource[S, T]{
		raw: &UnstructuredResource{cluster: c, def: rawDef},
	}, nil
}

func buildDefinition[S, T any](def ResourceDef[S, T]) (*resourceDefinition, error) {
	rawDef := &resourceDefinition{
		Resource:        def.Resource,
		APIVersion:      def.APIVersion,
		Kind:            def.Kind,
		annotationRules: make(map[string]AnnotationRule, len(def.Annotations)),
	}
	for _, rule := range def.Annotations {
		if strings.TrimSpace(rule.Key) == "" || strings.Contains(rule.Key, "\x00") {
			return nil, fmt.Errorf("%w: invalid annotation key", ErrInvalidResource)
		}
		if _, exists := rawDef.annotationRules[rule.Key]; exists {
			return nil, fmt.Errorf("%w: duplicate annotation rule %q", ErrInvalidResource, rule.Key)
		}
		rawDef.annotationRules[rule.Key] = rule
	}

	var err error
	rawDef.specRules, err = parseFieldRules("spec", typeOf[S]())
	if err != nil {
		return nil, err
	}
	rawDef.statusRules, err = parseFieldRules("status", typeOf[T]())
	if err != nil {
		return nil, err
	}
	rawDef.defaultObject = func(obj *Unstructured) error {
		applyAnnotationDefaults(rawDef, obj)
		if def.Default == nil {
			return nil
		}
		typed, err := unstructuredToTyped[S, T](obj)
		if err != nil {
			return err
		}
		if err := def.Default(typed); err != nil {
			return err
		}
		updated, err := typedToUnstructured(typed)
		if err != nil {
			return err
		}
		obj.Metadata = cloneMetadata(updated.Metadata)
		obj.Spec = cloneRaw(updated.Spec)
		applyAnnotationDefaults(rawDef, obj)
		return nil
	}
	rawDef.validateObject = func(oldObj, newObj *Unstructured, subresource Subresource) error {
		if err := validateAnnotationRules(rawDef, oldObj, newObj, subresource); err != nil {
			return err
		}
		if subresource == SubresourceStatus {
			if err := validateFieldRules(oldObj, newObj, rawDef.statusRules); err != nil {
				return err
			}
		} else if err := validateFieldRules(oldObj, newObj, rawDef.specRules); err != nil {
			return err
		}
		if def.Validate == nil {
			return nil
		}
		oldTyped, newTyped, err := convertValidationObjects[S, T](oldObj, newObj)
		if err != nil {
			return err
		}
		return def.Validate(oldTyped, newTyped, subresource)
	}
	return rawDef, nil
}

func validateDefinition(def *resourceDefinition) error {
	if def == nil {
		return ErrInvalidResource
	}
	if err := validateResourceName(def.Resource); err != nil {
		return err
	}
	if strings.TrimSpace(def.APIVersion) == "" || strings.Contains(def.APIVersion, "/../") {
		return fmt.Errorf("%w: invalid apiVersion", ErrInvalidResource)
	}
	if strings.TrimSpace(def.Kind) == "" || strings.Contains(def.Kind, "/") {
		return fmt.Errorf("%w: invalid kind", ErrInvalidResource)
	}
	return nil
}

func typeOf[T any]() reflect.Type {
	var zero T
	typ := reflect.TypeOf(zero)
	if typ == nil {
		return nil
	}
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	return typ
}

func parseFieldRules(prefix string, typ reflect.Type) ([]fieldRule, error) {
	if typ == nil || typ.Kind() != reflect.Struct {
		return nil, nil
	}
	rules := make([]fieldRule, 0)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.PkgPath != "" {
			continue
		}
		jsonName := jsonFieldName(field)
		if jsonName == "-" {
			continue
		}
		clusterTag := field.Tag.Get("cluster")
		if clusterTag == "" {
			continue
		}
		rule, err := parseClusterTag(prefix+"."+jsonName, clusterTag)
		if err != nil {
			return nil, err
		}
		rules = append(rules, rule)
	}
	return rules, nil
}

func jsonFieldName(field reflect.StructField) string {
	tag := field.Tag.Get("json")
	if tag == "-" {
		return "-"
	}
	name, _, _ := strings.Cut(tag, ",")
	if name != "" {
		return name
	}
	return field.Name
}

func parseClusterTag(path, tag string) (fieldRule, error) {
	rule := fieldRule{Path: path}
	for _, part := range strings.Split(tag, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, value, hasValue := strings.Cut(part, "=")
		switch key {
		case "required":
			rule.Required = true
		case "immutable":
			rule.Immutable = true
		case "index":
			rule.Indexed = true
			if hasValue {
				rule.IndexName = value
			}
		case "watch":
			rule.Watch = true
		case "enum":
			if !hasValue || value == "" {
				return fieldRule{}, fmt.Errorf("%w: invalid enum rule for %s", ErrInvalidResource, path)
			}
			rule.Enum = strings.Split(value, "|")
		default:
			return fieldRule{}, fmt.Errorf("%w: unknown cluster tag %q", ErrInvalidResource, part)
		}
	}
	return rule, nil
}

func applyAnnotationDefaults(def *resourceDefinition, obj *Unstructured) {
	if obj.Metadata.Annotations == nil {
		obj.Metadata.Annotations = Annotations{}
	}
	for key, rule := range def.annotationRules {
		if _, exists := obj.Metadata.Annotations[key]; !exists && rule.Default != "" {
			obj.Metadata.Annotations[key] = rule.Default
		}
	}
}

func validateAnnotationRules(def *resourceDefinition, oldObj, newObj *Unstructured, subresource Subresource) error {
	if subresource == SubresourceStatus {
		return nil
	}
	for key, rule := range def.annotationRules {
		value := ""
		if newObj.Metadata.Annotations != nil {
			value = newObj.Metadata.Annotations[key]
		}
		if rule.Required && value == "" {
			return fmt.Errorf("%w: annotation %q is required", ErrInvalidObject, key)
		}
		if oldObj != nil && rule.Immutable {
			oldValue := ""
			if oldObj.Metadata.Annotations != nil {
				oldValue = oldObj.Metadata.Annotations[key]
			}
			if oldValue != value {
				return fmt.Errorf("%w: annotation %q is immutable", ErrInvalidObject, key)
			}
		}
	}
	return nil
}

func validateFieldRules(oldObj, newObj *Unstructured, rules []fieldRule) error {
	for _, rule := range rules {
		newRaw, newExists := fieldRawValue(newObj, rule.Path)
		if rule.Required && (!newExists || isEmptyJSONValue(newRaw)) {
			return fmt.Errorf("%w: field %s is required", ErrInvalidObject, rule.Path)
		}
		if newExists && len(rule.Enum) > 0 && !isEmptyJSONValue(newRaw) {
			value, ok := rawScalarString(newRaw)
			if !ok || !slices.Contains(rule.Enum, value) {
				return fmt.Errorf("%w: field %s is outside enum", ErrInvalidObject, rule.Path)
			}
		}
		if oldObj != nil && rule.Immutable {
			oldRaw, oldExists := fieldRawValue(oldObj, rule.Path)
			if oldExists != newExists || !jsonEqual(oldRaw, newRaw) {
				return fmt.Errorf("%w: field %s is immutable", ErrInvalidObject, rule.Path)
			}
		}
	}
	return nil
}

func convertValidationObjects[S, T any](
	oldObj *Unstructured,
	newObj *Unstructured,
) (*Object[S, T], *Object[S, T], error) {
	var oldTyped *Object[S, T]
	if oldObj != nil {
		converted, err := unstructuredToTyped[S, T](oldObj)
		if err != nil {
			return nil, nil, err
		}
		oldTyped = converted
	}
	newTyped, err := unstructuredToTyped[S, T](newObj)
	if err != nil {
		return nil, nil, err
	}
	return oldTyped, newTyped, nil
}

func fieldRawValue(obj *Unstructured, path string) (json.RawMessage, bool) {
	switch path {
	case "metadata.name":
		return mustMarshalRaw(obj.Metadata.Name), true
	case "metadata.uid":
		return mustMarshalRaw(obj.Metadata.UID), true
	case "apiVersion":
		return mustMarshalRaw(obj.APIVersion), true
	case "kind":
		return mustMarshalRaw(obj.Kind), true
	}

	prefix, field, ok := strings.Cut(path, ".")
	if !ok || field == "" || strings.Contains(field, ".") {
		return nil, false
	}
	var raw json.RawMessage
	switch prefix {
	case "spec":
		raw = obj.Spec
	case "status":
		raw = obj.Status
	default:
		return nil, false
	}
	if len(raw) == 0 {
		return nil, false
	}
	values := map[string]json.RawMessage{}
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, false
	}
	value, exists := values[field]
	return cloneRaw(value), exists
}

func mustMarshalRaw(value any) json.RawMessage {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	return raw
}

func isEmptyJSONValue(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return true
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return false
	}
	switch v := value.(type) {
	case nil:
		return true
	case string:
		return v == ""
	default:
		return false
	}
}

func rawScalarString(raw json.RawMessage) (string, bool) {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", false
	}
	switch v := value.(type) {
	case string:
		return v, true
	case bool:
		if v {
			return "true", true
		}
		return "false", true
	case float64:
		return fmt.Sprint(v), true
	default:
		return "", false
	}
}
