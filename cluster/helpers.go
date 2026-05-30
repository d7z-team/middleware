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

func invalidPathToken(value string) bool {
	value = strings.TrimSpace(value)
	return value == "" || value == "." || value == ".." || strings.ContainsAny(value, `/\`)
}

func validateMetadata(meta Metadata) error {
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

func validateSpecPatch(patch []byte) error {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(patch, &root); err != nil {
		return err
	}
	if _, ok := root["status"]; ok {
		return fmt.Errorf("%w: status must be patched through status subresource", ErrInvalidObject)
	}
	if raw, ok := root["metadata"]; ok {
		var meta map[string]json.RawMessage
		if err := json.Unmarshal(raw, &meta); err != nil {
			return err
		}
		for key := range meta {
			switch key {
			case "labels", "annotations", "finalizers":
			default:
				return fmt.Errorf("%w: metadata.%s is managed", ErrInvalidObject, key)
			}
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
	return obj.APIVersion + "/" + obj.Kind + "/" + obj.Metadata.Name
}

func randomToken(prefix string, size int) (string, error) {
	buf := make([]byte, size)
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
