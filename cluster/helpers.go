package cluster

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

func validateResourceName(name string) error {
	if invalidPathToken(name) {
		return fmt.Errorf("%w: invalid resource", ErrInvalidResource)
	}
	return nil
}

func validateObjectName(name string) error {
	if invalidPathToken(name) {
		return fmt.Errorf("%w: invalid name", ErrInvalidObject)
	}
	return nil
}

func validateNamespace(namespace string) error {
	if invalidPathToken(namespace) {
		return fmt.Errorf("%w: invalid namespace", ErrInvalidObject)
	}
	return nil
}

func invalidPathToken(value string) bool {
	value = strings.TrimSpace(value)
	return value == "" || value == "." || value == ".." || strings.ContainsAny(value, `/\`)
}

func validateMetadataWithSchema(meta Metadata, _ map[string]any) error {
	if meta.Namespace != "" {
		if err := validateNamespace(meta.Namespace); err != nil {
			return err
		}
	}
	if err := validateMetadataKeys(meta.Labels); err != nil {
		return err
	}
	return validateMetadataKeys(meta.Annotations)
}

func validateMetadataKeys(values map[string]string) error {
	for key := range values {
		if strings.TrimSpace(key) == "" || strings.Contains(key, "\x00") {
			return fmt.Errorf("%w: invalid metadata key", ErrInvalidObject)
		}
	}
	return nil
}

func validateSpecPatch(patch []byte, writable map[string]struct{}) error {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(patch, &root); err != nil {
		return err
	}
	if _, ok := root["status"]; ok {
		return fmt.Errorf("%w: status must be patched through status subresource", ErrInvalidObject)
	}
	if raw, ok := root["metadata"]; ok {
		if err := validateMetadataPatchWithSchema(raw, writable); err != nil {
			return err
		}
	}
	return nil
}

func validateMetadataPatchWithSchema(patch []byte, writable map[string]struct{}) error {
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(patch, &meta); err != nil {
		return err
	}
	if len(meta) == 0 {
		return ErrInvalidObject
	}
	for key := range meta {
		if _, ok := writable[key]; !ok {
			return fmt.Errorf("%w: metadata.%s is managed", ErrInvalidObject, key)
		}
	}
	return nil
}

func validateRawObjectJSON(obj *Unstructured) error {
	if err := validateRawJSONField("spec", obj.Spec); err != nil {
		return err
	}
	return validateRawJSONField("status", obj.Status)
}

func validateRawJSONField(name string, raw json.RawMessage) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return fmt.Errorf("%w: invalid %s JSON: %v", ErrInvalidObject, name, err)
	}
	return nil
}

func updateRV(primary, fallback string) (uint64, error) {
	if primary == "" {
		primary = fallback
	}
	return parseRequiredRV(primary)
}

func parseRequiredRV(value string) (uint64, error) {
	rv, err := parseOptionalRV(value)
	if err != nil {
		return 0, err
	}
	if rv == 0 {
		return 0, ErrConflict
	}
	return rv, nil
}

func parseOptionalRV(value string) (uint64, error) {
	if value == "" {
		return 0, nil
	}
	rv, err := strconv.ParseUint(value, 10, 64)
	if err != nil || rv == 0 {
		return 0, fmt.Errorf("%w: invalid resourceVersion", ErrInvalidObject)
	}
	return rv, nil
}

func parseStoredRV(value string) uint64 {
	rv, _ := strconv.ParseUint(value, 10, 64)
	return rv
}

func formatRV(rv uint64) string {
	if rv == 0 {
		return ""
	}
	return strconv.FormatUint(rv, 10)
}

func rvKey(rv uint64) string {
	return fmt.Sprintf("%020d", rv)
}

func parseRVKey(key string) uint64 {
	key = key[strings.LastIndex(key, "/")+1:]
	rv, _ := strconv.ParseUint(key, 10, 64)
	return rv
}

func objectCursor(obj Unstructured) string {
	return obj.APIVersion + "/" + obj.Kind + "/" + obj.Metadata.Namespace + "/" + obj.Metadata.Name
}

func objectStorageKey(ref objectRef) string {
	if ref.Namespace == "" {
		return ref.Name
	}
	return ref.Namespace + "\x00" + ref.Name
}

func objectMatchesScope(obj Unstructured, scope resourceScope) bool {
	if scope.Namespace != "" {
		return obj.Metadata.Namespace == scope.Namespace
	}
	if scope.AllNamespaces {
		return obj.Metadata.Namespace != ""
	}
	return obj.Metadata.Namespace == ""
}

func eventMatchesScope(event resourceEvent, scope resourceScope) bool {
	if scope.Resource != "" && event.Ref.Resource != scope.Resource {
		return false
	}
	if scope.Namespace != "" {
		return event.Ref.Namespace == scope.Namespace
	}
	if scope.AllNamespaces {
		return event.Ref.Namespace != ""
	}
	return event.Ref.Namespace == ""
}

func randomToken(prefix string) (string, error) {
	buf := make([]byte, 18)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return prefix + "_" + base64.RawURLEncoding.EncodeToString(buf), nil
}

func cloneUnstructuredPtr(obj *Unstructured) *Unstructured {
	if obj == nil {
		return nil
	}
	copied := cloneUnstructured(*obj)
	return &copied
}

func cloneUnstructured(obj Unstructured) Unstructured {
	return Unstructured{
		APIVersion: obj.APIVersion,
		Kind:       obj.Kind,
		Metadata:   cloneMetadata(obj.Metadata),
		Spec:       cloneRaw(obj.Spec),
		Status:     cloneRaw(obj.Status),
	}
}

func cloneMetadata(meta Metadata) Metadata {
	copied := meta
	copied.DeletedAt = cloneTimePtr(meta.DeletedAt)
	copied.Labels = cloneLabels(meta.Labels)
	copied.Annotations = cloneAnnotations(meta.Annotations)
	copied.Finalizers = append([]string(nil), meta.Finalizers...)
	return copied
}

func cloneLabels(labels Labels) Labels {
	if labels == nil {
		return nil
	}
	copied := make(Labels, len(labels))
	for key, value := range labels {
		copied[key] = value
	}
	return copied
}

func cloneAnnotations(annotations Annotations) Annotations {
	if annotations == nil {
		return nil
	}
	copied := make(Annotations, len(annotations))
	for key, value := range annotations {
		copied[key] = value
	}
	return copied
}

func fieldRawValue(obj *Unstructured, path string) (json.RawMessage, bool) {
	switch path {
	case "metadata.namespace":
		return mustMarshalRaw(obj.Metadata.Namespace), true
	case "metadata.name":
		return mustMarshalRaw(obj.Metadata.Name), true
	}
	if strings.HasPrefix(path, "metadata.") {
		raw, err := json.Marshal(obj.Metadata)
		if err != nil {
			return nil, false
		}
		fields, ok := rawObjectFields(raw)
		if !ok {
			return nil, false
		}
		current, ok := fields[strings.TrimPrefix(path, "metadata.")]
		if ok {
			return current, true
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, false
		}
		currentValue := value
		for _, segment := range strings.Split(strings.TrimPrefix(path, "metadata."), ".") {
			object, ok := currentValue.(map[string]any)
			if !ok {
				return nil, false
			}
			currentValue, ok = object[segment]
			if !ok {
				return nil, false
			}
		}
		out, err := json.Marshal(currentValue)
		if err != nil {
			return nil, false
		}
		return out, true
	}

	prefix, field, ok := strings.Cut(path, ".")
	if !ok || field == "" {
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
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, false
	}
	current := value
	for _, segment := range strings.Split(field, ".") {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[segment]
		if !ok {
			return nil, false
		}
	}
	out, err := json.Marshal(current)
	if err != nil {
		return nil, false
	}
	return out, true
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

func applyDefaultRules(obj *Unstructured, rules []defaultRule) error {
	for _, rule := range rules {
		if err := applyDefaultRule(obj, rule); err != nil {
			return err
		}
	}
	return nil
}

func applyDefaultRule(obj *Unstructured, rule defaultRule) error {
	root, remainder, ok := strings.Cut(rule.Path, ".")
	if !ok || remainder == "" {
		return nil
	}
	var current json.RawMessage
	switch root {
	case "spec":
		current = obj.Spec
	case "status":
		current = obj.Status
	default:
		return nil
	}
	value := map[string]any{}
	if len(current) > 0 && string(current) != "null" {
		if err := json.Unmarshal(current, &value); err != nil {
			return err
		}
	}
	if applyDefaultMap(value, strings.Split(remainder, "."), rule.Value) {
		updated, err := json.Marshal(value)
		if err != nil {
			return err
		}
		switch root {
		case "spec":
			obj.Spec = updated
		case "status":
			obj.Status = updated
		}
	}
	return nil
}

func pruneRawWithSchema(raw json.RawMessage, schema map[string]any) (json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 || string(raw) == "null" || schema == nil {
		return cloneRaw(raw), nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	pruned, err := pruneValueWithSchema(value, schema)
	if err != nil {
		return nil, err
	}
	if pruned == nil {
		return nil, nil
	}
	out, err := json.Marshal(pruned)
	if err != nil {
		return nil, err
	}
	if string(out) == "null" {
		return nil, nil
	}
	return out, nil
}

func pruneValueWithSchema(value any, schema map[string]any) (any, error) {
	if boolValue(schema["x-cluster-preserve-unknown-fields"]) {
		return value, nil
	}
	switch schema["type"] {
	case "object":
		object, ok := value.(map[string]any)
		if !ok {
			return value, nil
		}
		result := make(map[string]any, len(object))
		properties, _ := schema["properties"].(map[string]any)
		for key, childRaw := range properties {
			childSchema, ok := childRaw.(map[string]any)
			if !ok {
				continue
			}
			childValue, exists := object[key]
			if !exists {
				continue
			}
			pruned, err := pruneValueWithSchema(childValue, childSchema)
			if err != nil {
				return nil, err
			}
			result[key] = pruned
		}
		if additional, ok := schema["additionalProperties"].(map[string]any); ok {
			for key, childValue := range object {
				if _, exists := properties[key]; exists {
					continue
				}
				pruned, err := pruneValueWithSchema(childValue, additional)
				if err != nil {
					return nil, err
				}
				result[key] = pruned
			}
		}
		return result, nil
	case "array":
		itemsSchema, _ := schema["items"].(map[string]any)
		items, ok := value.([]any)
		if !ok || itemsSchema == nil {
			return value, nil
		}
		result := make([]any, 0, len(items))
		for _, item := range items {
			pruned, err := pruneValueWithSchema(item, itemsSchema)
			if err != nil {
				return nil, err
			}
			result = append(result, pruned)
		}
		return result, nil
	default:
		return value, nil
	}
}

func applyDefaultMap(root map[string]any, segments []string, raw json.RawMessage) bool {
	if len(segments) == 0 {
		return false
	}
	if len(segments) == 1 {
		if _, exists := root[segments[0]]; exists {
			return false
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return false
		}
		root[segments[0]] = value
		return true
	}
	next, ok := root[segments[0]].(map[string]any)
	if !ok {
		next = map[string]any{}
		root[segments[0]] = next
	}
	return applyDefaultMap(next, segments[1:], raw)
}

func cloneRaw(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return nil
	}
	return append(json.RawMessage(nil), raw...)
}

func cloneTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func ensureMetadataMaps(meta *Metadata) {
	if meta.Labels == nil {
		meta.Labels = Labels{}
	}
	if meta.Annotations == nil {
		meta.Annotations = Annotations{}
	}
}

func jsonEqual(a, b json.RawMessage) bool {
	if len(bytes.TrimSpace(a)) == 0 {
		a = nil
	}
	if len(bytes.TrimSpace(b)) == 0 {
		b = nil
	}
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	var left any
	var right any
	if json.Unmarshal(a, &left) != nil || json.Unmarshal(b, &right) != nil {
		return bytes.Equal(a, b)
	}
	return reflect.DeepEqual(left, right)
}

func changedPaths(oldObj, newObj *Unstructured, subresource Subresource) []string {
	changed := make([]string, 0)
	if oldObj == nil {
		if newObj == nil {
			return nil
		}
		changed = append(changed, "metadata")
		if len(newObj.Spec) > 0 {
			changed = append(changed, changedJSONPaths("spec", nil, newObj.Spec)...)
		}
		if len(newObj.Status) > 0 {
			changed = append(changed, changedJSONPaths("status", nil, newObj.Status)...)
		}
		return sortedUnique(changed)
	}
	if newObj == nil {
		return nil
	}
	if subresource == SubresourceStatus {
		return sortedUnique(changedJSONPaths("status", oldObj.Status, newObj.Status))
	}
	if !reflect.DeepEqual(oldObj.Metadata.Labels, newObj.Metadata.Labels) {
		changed = append(changed, "metadata.labels")
	}
	if !reflect.DeepEqual(oldObj.Metadata.Annotations, newObj.Metadata.Annotations) {
		changed = append(changed, "metadata.annotations")
	}
	if !reflect.DeepEqual(oldObj.Metadata.Finalizers, newObj.Metadata.Finalizers) {
		changed = append(changed, "metadata.finalizers")
	}
	if oldObj.Metadata.Namespace != newObj.Metadata.Namespace {
		changed = append(changed, "metadata.namespace")
	}
	if (oldObj.Metadata.DeletedAt == nil) != (newObj.Metadata.DeletedAt == nil) ||
		oldObj.Metadata.DeletedAt != nil && !oldObj.Metadata.DeletedAt.Equal(*newObj.Metadata.DeletedAt) {
		changed = append(changed, "metadata.deletedAt")
	}
	changed = append(changed, changedJSONPaths("spec", oldObj.Spec, newObj.Spec)...)
	return sortedUnique(changed)
}

func changedJSONPaths(prefix string, oldRaw, newRaw json.RawMessage) []string {
	if jsonEqual(oldRaw, newRaw) {
		return nil
	}
	oldValues, oldOK := rawObjectFields(oldRaw)
	newValues, newOK := rawObjectFields(newRaw)
	if !oldOK || !newOK {
		return []string{prefix}
	}
	keys := make(map[string]struct{}, len(oldValues)+len(newValues))
	for key := range oldValues {
		keys[key] = struct{}{}
	}
	for key := range newValues {
		keys[key] = struct{}{}
	}
	changed := make([]string, 0, len(keys))
	for key := range keys {
		if !jsonEqual(oldValues[key], newValues[key]) {
			changed = append(changed, prefix+"."+key)
		}
	}
	return changed
}

func rawObjectFields(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return map[string]json.RawMessage{}, true
	}
	values := map[string]json.RawMessage{}
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, false
	}
	for key, value := range values {
		values[key] = cloneRaw(value)
	}
	return values, true
}

func sortedUnique(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	sort.Strings(values)
	out := values[:0]
	for _, value := range values {
		if value == "" {
			continue
		}
		if len(out) == 0 || out[len(out)-1] != value {
			out = append(out, value)
		}
	}
	return out
}

func cloneEvent(event resourceEvent) resourceEvent {
	return resourceEvent{
		Type:            event.Type,
		ResourceVersion: event.ResourceVersion,
		Ref:             event.Ref,
		Object:          cloneUnstructuredPtr(event.Object),
		Annotations:     cloneAnnotations(event.Annotations),
		Changed:         append([]string(nil), event.Changed...),
	}
}

func newStoreEvent(req commitRequest, rv uint64, obj *Unstructured) resourceEvent {
	return resourceEvent{
		Type:            req.EventType,
		ResourceVersion: formatRV(rv),
		Ref:             req.Ref,
		Object:          cloneUnstructuredPtr(obj),
		Annotations:     cloneAnnotations(req.EventAnnotations),
		Changed:         append([]string(nil), req.Changed...),
	}
}

func normalizeStorePrefix(prefix string) string {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return ""
	}
	return prefix + "/"
}

func lockKeyFromRef(ref objectRef) string {
	return ref.Resource + "\x00" + ref.Namespace + "\x00" + ref.Name
}

func decodeAdmissionRequest(obj Unstructured) (AdmissionRequestSpec, AdmissionRequestStatus, error) {
	typed, err := unstructuredToTyped[AdmissionRequestSpec, AdmissionRequestStatus](&obj)
	if err != nil {
		return AdmissionRequestSpec{}, AdmissionRequestStatus{}, err
	}
	return typed.Spec, typed.Status, nil
}

func encodeAdmissionRequest(meta Metadata, spec AdmissionRequestSpec, status AdmissionRequestStatus) (*Unstructured, error) {
	return typedToUnstructured(&Object[AdmissionRequestSpec, AdmissionRequestStatus]{
		APIVersion: "cluster.d7z.net/v1",
		Kind:       "AdmissionRequest",
		Metadata:   meta,
		Spec:       spec,
		Status:     status,
	})
}

func admissionTargetCommit(spec AdmissionRequestSpec) (commitRequest, *Unstructured, error) {
	if spec.Object == nil {
		return commitRequest{}, nil, ErrInvalidObject
	}
	target := cloneUnstructured(*spec.Object)
	ref := objectRef{Resource: spec.Resource, Namespace: spec.Namespace, Name: spec.Name}
	expectedRV, err := parseOptionalRV(spec.Precondition.ResourceVersion)
	if err != nil {
		return commitRequest{}, nil, err
	}
	req := commitRequest{
		Ref:              ref,
		Object:           &target,
		EventAnnotations: cloneAnnotations(spec.EventAnnotations),
	}
	switch spec.Operation {
	case AdmissionCreate:
		req.Op = commitCreate
		req.EventType = WatchAdded
		req.Changed = changedPaths(nil, &target, SubresourceSpec)
	case AdmissionUpdate:
		if spec.OldObject == nil {
			return commitRequest{}, nil, ErrInvalidObject
		}
		req.Op = commitUpdate
		req.ExpectedRV = expectedRV
		req.EventType = WatchModified
		req.Changed = changedPaths(spec.OldObject, &target, spec.Subresource)
	case AdmissionDelete:
		if spec.OldObject == nil {
			return commitRequest{}, nil, ErrInvalidObject
		}
		req.ExpectedRV = expectedRV
		if len(spec.OldObject.Metadata.Finalizers) > 0 && spec.OldObject.Metadata.DeletedAt == nil {
			req.Op = commitUpdate
			req.EventType = WatchModified
			req.Changed = changedPaths(spec.OldObject, &target, SubresourceSpec)
		} else {
			req.Op = commitDelete
			req.EventType = WatchDeleted
			req.Changed = changedPaths(spec.OldObject, &target, SubresourceSpec)
		}
	default:
		return commitRequest{}, nil, ErrInvalidObject
	}
	return req, &target, nil
}
