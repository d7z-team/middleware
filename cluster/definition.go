package cluster

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
)

type ResourceDef struct {
	Resource   string
	APIVersion string
	Kind       string
	Namespaced bool

	Schema    json.RawMessage
	Admission []AdmissionRule
}

type TypedResourceDef[S, T any] struct {
	Resource   string
	APIVersion string
	Kind       string
	Namespaced bool

	Schema    json.RawMessage
	Admission []AdmissionRule
}

type resourceDefinition struct {
	Resource   string
	APIVersion string
	Kind       string
	Builtin    bool
	Namespaced bool

	Schema            json.RawMessage
	SchemaFingerprint string
	Admission         []AdmissionRule
	Indexes           []IndexInfo
	admissionMatches  map[string][]AdmissionRule
	metadataRules     []fieldRule
	specRules         []fieldRule
	statusRules       []fieldRule
	defaultRules      []defaultRule
	metadataSchema    map[string]any
	specSchema        map[string]any
	statusSchema      map[string]any
	metadataWritable  map[string]struct{}

	defaultObject         func(*Unstructured) error
	validateObject        func(oldObj, newObj *Unstructured, subresource Subresource) error
	pruneObject           func(*Unstructured) error
	validateMetadata      func(Metadata) error
	validateMetadataPatch func([]byte) error
}

type defaultRule struct {
	Path  string
	Value json.RawMessage
}

func DefineResource(c *Cluster, def ResourceDef) (*UnstructuredResource, error) {
	if c == nil {
		return nil, ErrInvalidConfig
	}
	rawDef, err := buildResourceDefinition(def)
	if err != nil {
		return nil, err
	}
	if err := c.registerDefinition(rawDef); err != nil {
		return nil, err
	}
	return &UnstructuredResource{cluster: c, def: rawDef}, nil
}

func Define[S, T any](c *Cluster, def TypedResourceDef[S, T]) (*Resource[S, T], error) {
	if c == nil {
		return nil, ErrInvalidConfig
	}
	rawDef, err := buildTypedDefinition(def)
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

func SchemaFrom[S, T any](apiVersion, kind string, namespaced bool) (json.RawMessage, error) {
	specSchema, specIndexes, err := schemaForType(typeOf[S](), "spec")
	if err != nil {
		return nil, err
	}
	statusSchema, _, err := schemaForType(typeOf[T](), "status")
	if err != nil {
		return nil, err
	}
	root := map[string]any{
		"$schema": "https://json-schema.org/draft/2020-12/schema",
		"type":    "object",
		"required": []string{
			"apiVersion",
			"kind",
			"metadata",
			"spec",
		},
		"properties": map[string]any{
			"apiVersion": map[string]any{
				"type":    "string",
				"default": apiVersion,
			},
			"kind": map[string]any{
				"type":    "string",
				"default": kind,
			},
			"metadata": metadataSchema(namespaced),
			"spec":     specSchema,
			"status":   statusSchema,
		},
	}
	if isEmptyObjectSchema(statusSchema) {
		root["properties"].(map[string]any)["status"] = map[string]any{
			"type":                 "object",
			"additionalProperties": false,
		}
	}
	if len(specIndexes) > 0 {
		root["x-cluster-indexes"] = specIndexes
	}
	return canonicalJSON(root)
}

func buildResourceDefinition(def ResourceDef) (*resourceDefinition, error) {
	compiledSchema, indexes, metadataRules, specRules, statusRules, defaults, metadataSchema, specSchema, statusSchema, metadataWritable, err := compileSchema(def.Schema)
	if err != nil {
		return nil, err
	}
	fp := schemaFingerprint(compiledSchema)
	resourceDef := &resourceDefinition{
		Resource:          def.Resource,
		APIVersion:        def.APIVersion,
		Kind:              def.Kind,
		Namespaced:        def.Namespaced,
		Schema:            compiledSchema,
		SchemaFingerprint: fp,
		Admission:         cloneAdmissionRules(def.Admission),
		admissionMatches:  compileAdmissionRules(def.Admission),
		Indexes:           indexes,
		metadataRules:     metadataRules,
		specRules:         specRules,
		statusRules:       statusRules,
		defaultRules:      defaults,
		metadataSchema:    metadataSchema,
		specSchema:        specSchema,
		statusSchema:      statusSchema,
		metadataWritable:  metadataWritable,
	}
	resourceDef.defaultObject = func(obj *Unstructured) error {
		return applyDefaultRules(obj, resourceDef.defaultRules)
	}
	resourceDef.pruneObject = func(obj *Unstructured) error {
		spec, err := pruneRawWithSchema(obj.Spec, resourceDef.specSchema)
		if err != nil {
			return err
		}
		status, err := pruneRawWithSchema(obj.Status, resourceDef.statusSchema)
		if err != nil {
			return err
		}
		obj.Spec = spec
		obj.Status = status
		return nil
	}
	resourceDef.validateObject = func(oldObj, newObj *Unstructured, subresource Subresource) error {
		switch subresource {
		case SubresourceStatus:
			return validateFieldRules(oldObj, newObj, resourceDef.statusRules)
		case SubresourceMetadata:
			return validateFieldRules(oldObj, newObj, resourceDef.metadataRules)
		default:
			if err := validateFieldRules(oldObj, newObj, resourceDef.metadataRules); err != nil {
				return err
			}
			return validateFieldRules(oldObj, newObj, resourceDef.specRules)
		}
	}
	resourceDef.validateMetadata = func(meta Metadata) error {
		return validateMetadataWithSchema(meta, resourceDef.metadataSchema)
	}
	resourceDef.validateMetadataPatch = func(patch []byte) error {
		return validateMetadataPatchWithSchema(patch, resourceDef.metadataWritable)
	}
	return resourceDef, validateDefinition(resourceDef)
}

func buildTypedDefinition[S, T any](def TypedResourceDef[S, T]) (*resourceDefinition, error) {
	schema := def.Schema
	if len(strings.TrimSpace(string(schema))) == 0 {
		var err error
		schema, err = SchemaFrom[S, T](def.APIVersion, def.Kind, def.Namespaced)
		if err != nil {
			return nil, err
		}
	}
	rawDef, err := buildResourceDefinition(ResourceDef{
		Resource:   def.Resource,
		APIVersion: def.APIVersion,
		Kind:       def.Kind,
		Namespaced: def.Namespaced,
		Schema:     schema,
		Admission:  def.Admission,
	})
	if err != nil {
		return nil, err
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
	if len(def.Schema) == 0 {
		return fmt.Errorf("%w: empty schema", ErrInvalidResource)
	}
	for _, rule := range def.Admission {
		if strings.TrimSpace(rule.Name) == "" {
			return fmt.Errorf("%w: invalid admission rule", ErrInvalidResource)
		}
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

func metadataSchema(namespaced bool) map[string]any {
	properties := map[string]any{
		"name":            map[string]any{"type": "string"},
		"uid":             map[string]any{"type": "string"},
		"resourceVersion": map[string]any{"type": "string"},
		"generation":      map[string]any{"type": "integer"},
		"createdAt":       map[string]any{"type": "string", "format": "date-time"},
		"updatedAt":       map[string]any{"type": "string", "format": "date-time"},
		"deletedAt":       map[string]any{"type": "string", "format": "date-time"},
		"labels": map[string]any{
			"type":                        "object",
			"additionalProperties":        map[string]any{"type": "string"},
			"x-cluster-index-keys":        []string{},
			"x-cluster-metadata-writable": true,
		},
		"annotations": map[string]any{
			"type":                        "object",
			"additionalProperties":        map[string]any{"type": "string"},
			"x-cluster-index-keys":        []string{},
			"x-cluster-metadata-writable": true,
		},
		"finalizers": map[string]any{
			"type":                        "array",
			"items":                       map[string]any{"type": "string"},
			"x-cluster-metadata-writable": true,
		},
	}
	if namespaced {
		properties["namespace"] = map[string]any{"type": "string"}
	}
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{"name"},
		"properties":           properties,
	}
}

func schemaForType(typ reflect.Type, prefix string) (map[string]any, []IndexInfo, error) {
	if typ == nil {
		return map[string]any{
			"type":                 "object",
			"additionalProperties": false,
		}, nil, nil
	}
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf("%w: resource root must be struct", ErrInvalidResource)
	}
	properties := map[string]any{}
	required := make([]string, 0)
	indexes := make([]IndexInfo, 0)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.PkgPath != "" {
			continue
		}
		name, requiredField := jsonFieldName(field)
		if name == "-" {
			continue
		}
		fieldSchema, fieldIndexes, err := schemaForField(field, prefix+"."+name)
		if err != nil {
			return nil, nil, err
		}
		properties[name] = fieldSchema
		indexes = append(indexes, fieldIndexes...)
		if requiredField {
			required = append(required, name)
		}
	}
	schema := map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"properties":           properties,
	}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema, indexes, nil
}

func schemaForField(field reflect.StructField, path string) (map[string]any, []IndexInfo, error) {
	typ := field.Type
	nullable := false
	for typ.Kind() == reflect.Pointer {
		nullable = true
		typ = typ.Elem()
	}
	schema, err := schemaForValueType(typ, path)
	if err != nil {
		return nil, nil, err
	}
	clusterOptions, err := parseSchemaClusterTag(field.Tag.Get("cluster"))
	if err != nil {
		return nil, nil, err
	}
	indexes := make([]IndexInfo, 0, 1)
	if clusterOptions.index {
		schema["x-cluster-index"] = true
		indexes = append(indexes, IndexInfo{Path: path})
	}
	if clusterOptions.immutable {
		schema["x-cluster-immutable"] = true
	}
	if len(clusterOptions.enum) > 0 {
		enumValues := make([]any, 0, len(clusterOptions.enum))
		for _, value := range clusterOptions.enum {
			enumValues = append(enumValues, value)
		}
		schema["enum"] = enumValues
	}
	if clusterOptions.defaultValue != nil {
		schema["default"] = clusterOptions.defaultValue
	}
	if clusterOptions.preserveUnknown {
		schema["x-cluster-preserve-unknown-fields"] = true
	}
	if nullable {
		schema["nullable"] = true
	}
	return schema, indexes, nil
}

func schemaForValueType(typ reflect.Type, path string) (map[string]any, error) {
	switch typ.Kind() {
	case reflect.Struct:
		if typ.PkgPath() == "time" && typ.Name() == "Time" {
			return map[string]any{"type": "string", "format": "date-time"}, nil
		}
		schema, _, err := schemaForType(typ, path)
		return schema, err
	case reflect.String:
		return map[string]any{"type": "string"}, nil
	case reflect.Bool:
		return map[string]any{"type": "boolean"}, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return map[string]any{"type": "integer"}, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer", "minimum": 0}, nil
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}, nil
	case reflect.Slice:
		if typ.Elem().Kind() == reflect.Uint8 && typ.PkgPath() == "encoding/json" && typ.Name() == "RawMessage" {
			return map[string]any{
				"type":                              "object",
				"x-cluster-preserve-unknown-fields": true,
			}, nil
		}
		items, err := schemaForValueType(typ.Elem(), path)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "array", "items": items}, nil
	case reflect.Array:
		items, err := schemaForValueType(typ.Elem(), path)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "array", "items": items}, nil
	case reflect.Map:
		if typ.Key().Kind() != reflect.String {
			return nil, fmt.Errorf("%w: map key must be string", ErrInvalidResource)
		}
		valueSchema, err := schemaForValueType(typ.Elem(), path)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "object", "additionalProperties": valueSchema}, nil
	case reflect.Interface:
		return map[string]any{
			"type":                              "object",
			"x-cluster-preserve-unknown-fields": true,
		}, nil
	default:
		return nil, fmt.Errorf("%w: unsupported field type %s", ErrInvalidResource, typ.String())
	}
}

type clusterSchemaTag struct {
	enum            []string
	index           bool
	immutable       bool
	preserveUnknown bool
	defaultValue    any
}

func parseSchemaClusterTag(tag string) (clusterSchemaTag, error) {
	var out clusterSchemaTag
	for _, part := range strings.Split(tag, ",") {
		part = strings.TrimSpace(part)
		if part == "" || part == "required" || part == "nullable" {
			continue
		}
		key, value, hasValue := strings.Cut(part, "=")
		switch key {
		case "immutable":
			out.immutable = true
		case "index":
			out.index = true
		case "enum":
			if !hasValue || value == "" {
				return clusterSchemaTag{}, fmt.Errorf("%w: invalid enum", ErrInvalidResource)
			}
			out.enum = strings.Split(value, "|")
		case "default":
			if !hasValue {
				return clusterSchemaTag{}, fmt.Errorf("%w: invalid default", ErrInvalidResource)
			}
			out.defaultValue = value
		case "preserveUnknown":
			out.preserveUnknown = true
		default:
			return clusterSchemaTag{}, fmt.Errorf("%w: unknown cluster tag %q", ErrInvalidResource, part)
		}
	}
	return out, nil
}

func jsonFieldName(field reflect.StructField) (string, bool) {
	tag := field.Tag.Get("json")
	if tag == "-" {
		return "-", false
	}
	name, options, _ := strings.Cut(tag, ",")
	if name == "" {
		name = field.Name
	}
	required := true
	for _, option := range strings.Split(options, ",") {
		if option == "omitempty" {
			required = false
			break
		}
	}
	if field.Type.Kind() == reflect.Pointer {
		required = false
	}
	return name, required
}

func compileSchema(raw json.RawMessage) (json.RawMessage, []IndexInfo, []fieldRule, []fieldRule, []fieldRule, []defaultRule, map[string]any, map[string]any, map[string]any, map[string]struct{}, error) {
	var value map[string]any
	if len(raw) == 0 {
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, fmt.Errorf("%w: empty schema", ErrInvalidResource)
	}
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, fmt.Errorf("%w: invalid schema", ErrInvalidResource)
	}
	if value["type"] != "object" {
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, fmt.Errorf("%w: root schema must be object", ErrInvalidResource)
	}
	properties, _ := value["properties"].(map[string]any)
	for _, key := range []string{"apiVersion", "kind", "metadata", "spec"} {
		if _, ok := properties[key]; !ok {
			return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, fmt.Errorf("%w: schema missing %s", ErrInvalidResource, key)
		}
	}
	if err := validateStructuralNode("", value); err != nil {
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, err
	}
	indexes := make([]IndexInfo, 0)
	metadataRules := make([]fieldRule, 0)
	specRules := make([]fieldRule, 0)
	statusRules := make([]fieldRule, 0)
	defaults := make([]defaultRule, 0)
	var metadataSchema map[string]any
	var specSchema map[string]any
	var statusSchema map[string]any
	metadataWritable := make(map[string]struct{})
	collectSchemaIndexes("", value, &indexes)
	if metadata, ok := properties["metadata"].(map[string]any); ok {
		metadataSchema = metadata
		collectMetadataWritable(metadata, metadataWritable)
		if err := collectSchemaRules("metadata", metadata, &metadataRules, &defaults); err != nil {
			return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, err
		}
	}
	if spec, ok := properties["spec"].(map[string]any); ok {
		specSchema = spec
		if err := collectSchemaRules("spec", spec, &specRules, &defaults); err != nil {
			return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, err
		}
	}
	if status, ok := properties["status"].(map[string]any); ok {
		statusSchema = status
		if err := collectSchemaRules("status", status, &statusRules, &defaults); err != nil {
			return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, err
		}
	} else {
		statusSchema = map[string]any{
			"type":                 "object",
			"additionalProperties": false,
		}
	}
	compiled, err := canonicalJSON(value)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, err
	}
	return compiled, indexes, metadataRules, specRules, statusRules, defaults, metadataSchema, specSchema, statusSchema, metadataWritable, nil
}

func collectMetadataWritable(node map[string]any, out map[string]struct{}) {
	properties, _ := node["properties"].(map[string]any)
	for key, raw := range properties {
		child, ok := raw.(map[string]any)
		if ok && boolValue(child["x-cluster-metadata-writable"]) {
			out[key] = struct{}{}
		}
	}
}

func compileAdmissionRules(rules []AdmissionRule) map[string][]AdmissionRule {
	if len(rules) == 0 {
		return nil
	}
	out := make(map[string][]AdmissionRule)
	allOps := []AdmissionOperation{AdmissionCreate, AdmissionUpdate, AdmissionDelete}
	for _, rule := range rules {
		ops := rule.Operations
		if len(ops) == 0 {
			ops = allOps
		}
		subs := rule.Subresources
		if len(subs) == 0 {
			subs = []Subresource{SubresourceSpec}
		}
		for _, op := range ops {
			for _, sub := range subs {
				key := admissionRuleKey(op, sub)
				out[key] = append(out[key], rule)
			}
		}
	}
	return out
}

func admissionRuleKey(operation AdmissionOperation, subresource Subresource) string {
	return string(operation) + "|" + string(subresource)
}

func (d *resourceDefinition) admissionRules(operation AdmissionOperation, subresource Subresource) []AdmissionRule {
	if d == nil || len(d.admissionMatches) == 0 {
		return nil
	}
	rules := d.admissionMatches[admissionRuleKey(operation, subresource)]
	if len(rules) == 0 {
		return nil
	}
	return append([]AdmissionRule(nil), rules...)
}

func collectSchemaIndexes(path string, node map[string]any, indexes *[]IndexInfo) {
	if indexed, _ := node["x-cluster-index"].(bool); indexed && path != "" {
		*indexes = append(*indexes, IndexInfo{Path: path})
	}
	properties, _ := node["properties"].(map[string]any)
	for name, raw := range properties {
		child, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		next := name
		if path != "" {
			next = path + "." + name
		}
		collectSchemaIndexes(next, child, indexes)
	}
}

func validateStructuralNode(path string, node map[string]any) error {
	if ref, ok := node["$ref"].(string); ok {
		if !strings.HasPrefix(ref, "#/$defs/") {
			return fmt.Errorf("%w: only local $defs refs are allowed", ErrInvalidResource)
		}
	}
	nodeType, _ := node["type"].(string)
	if keys, exists := node["x-cluster-index-keys"]; exists {
		list, ok := keys.([]any)
		if !ok || nodeType != "object" || (path != "metadata.labels" && path != "metadata.annotations") {
			return fmt.Errorf("%w: x-cluster-index-keys only allowed on metadata labels/annotations", ErrInvalidResource)
		}
		_ = list
	}
	switch nodeType {
	case "object":
		if boolValue(node["x-cluster-preserve-unknown-fields"]) {
			return nil
		}
		properties, _ := node["properties"].(map[string]any)
		for name, raw := range properties {
			child, ok := raw.(map[string]any)
			if !ok {
				return fmt.Errorf("%w: invalid schema node", ErrInvalidResource)
			}
			childPath := name
			if path != "" {
				childPath = path + "." + name
			}
			if err := validateStructuralNode(childPath, child); err != nil {
				return err
			}
		}
		if additional, ok := node["additionalProperties"].(map[string]any); ok {
			childPath := path + ".*"
			if err := validateStructuralNode(childPath, additional); err != nil {
				return err
			}
		}
	case "array":
		items, ok := node["items"].(map[string]any)
		if !ok {
			return fmt.Errorf("%w: array %s must declare items", ErrInvalidResource, path)
		}
		return validateStructuralNode(path+"[]", items)
	case "string", "boolean", "integer", "number":
	default:
		return fmt.Errorf("%w: schema node %s missing or invalid type", ErrInvalidResource, path)
	}
	if boolValue(node["x-cluster-index"]) && (nodeType == "object" || nodeType == "array") {
		return fmt.Errorf("%w: x-cluster-index requires scalar field", ErrInvalidResource)
	}
	if boolValue(node["x-cluster-immutable"]) && strings.HasPrefix(path, "status") {
		return fmt.Errorf("%w: status fields cannot be immutable", ErrInvalidResource)
	}
	return nil
}

func collectSchemaRules(path string, node map[string]any, rules *[]fieldRule, defaults *[]defaultRule) error {
	required := map[string]struct{}{}
	if values, ok := node["required"].([]any); ok {
		for _, value := range values {
			name, ok := value.(string)
			if ok {
				required[name] = struct{}{}
			}
		}
	}
	properties, _ := node["properties"].(map[string]any)
	for name, raw := range properties {
		child, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		childPath := path + "." + name
		rule := fieldRule{
			Path:      childPath,
			Required:  hasRequired(required, name),
			Immutable: boolValue(child["x-cluster-immutable"]),
		}
		if enumValues, ok := child["enum"].([]any); ok {
			for _, value := range enumValues {
				text, ok := value.(string)
				if !ok {
					return fmt.Errorf("%w: enum must contain strings", ErrInvalidResource)
				}
				rule.Enum = append(rule.Enum, text)
			}
		}
		if rule.Required || rule.Immutable || len(rule.Enum) > 0 {
			*rules = append(*rules, rule)
		}
		if defaultValue, ok := child["default"]; ok {
			rawDefault, err := canonicalJSON(defaultValue)
			if err != nil {
				return err
			}
			*defaults = append(*defaults, defaultRule{Path: childPath, Value: rawDefault})
		}
		if err := validateNodeDefault(child); err != nil {
			return err
		}
		if err := collectSchemaRules(childPath, child, rules, defaults); err != nil {
			return err
		}
	}
	return nil
}

func validateNodeDefault(node map[string]any) error {
	defaultValue, ok := node["default"]
	if !ok {
		return nil
	}
	switch node["type"] {
	case "string":
		if _, ok := defaultValue.(string); !ok {
			return fmt.Errorf("%w: default type mismatch", ErrInvalidResource)
		}
	case "boolean":
		if _, ok := defaultValue.(bool); !ok {
			return fmt.Errorf("%w: default type mismatch", ErrInvalidResource)
		}
	case "integer", "number":
		if _, ok := defaultValue.(float64); !ok {
			return fmt.Errorf("%w: default type mismatch", ErrInvalidResource)
		}
	}
	return nil
}

func hasRequired(values map[string]struct{}, key string) bool {
	_, ok := values[key]
	return ok
}

func boolValue(value any) bool {
	ok, _ := value.(bool)
	return ok
}

func schemaFingerprint(schema json.RawMessage) string {
	sum := sha256.Sum256(schema)
	return hex.EncodeToString(sum[:])
}

func canonicalJSON(value any) (json.RawMessage, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var normalized any
	if err := json.Unmarshal(raw, &normalized); err != nil {
		return nil, err
	}
	raw, err = json.Marshal(normalized)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
}

func isEmptyObjectSchema(schema map[string]any) bool {
	properties, _ := schema["properties"].(map[string]any)
	required, _ := schema["required"].([]string)
	return len(properties) == 0 && len(required) == 0
}

func cloneAdmissionRules(in []AdmissionRule) []AdmissionRule {
	if len(in) == 0 {
		return nil
	}
	out := make([]AdmissionRule, 0, len(in))
	for _, rule := range in {
		out = append(out, AdmissionRule{
			Name:         rule.Name,
			Operations:   append([]AdmissionOperation(nil), rule.Operations...),
			Subresources: append([]Subresource(nil), rule.Subresources...),
			Timeout:      rule.Timeout,
		})
	}
	return out
}

func definitionEquivalent(a, b *resourceDefinition) bool {
	return a.Resource == b.Resource &&
		a.APIVersion == b.APIVersion &&
		a.Kind == b.Kind &&
		a.Namespaced == b.Namespaced &&
		a.SchemaFingerprint == b.SchemaFingerprint &&
		admissionRulesEqual(a.Admission, b.Admission)
}

func admissionRulesEqual(a, b []AdmissionRule) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].Timeout != b[i].Timeout {
			return false
		}
		if !slices.Equal(a[i].Operations, b[i].Operations) {
			return false
		}
		if !slices.Equal(a[i].Subresources, b[i].Subresources) {
			return false
		}
	}
	return true
}

type fieldRule struct {
	Path      string
	Required  bool
	Immutable bool
	Enum      []string
}

func validateFieldRules(oldObj, newObj *Unstructured, rules []fieldRule) error {
	for _, rule := range rules {
		newRaw, newExists := fieldRawValue(newObj, rule.Path)
		if rule.Required && !newExists {
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
