package cluster

import (
	"encoding/json"
)

func applyObjectPatch(oldObj Unstructured, patch []byte) (Unstructured, error) {
	raw, err := json.Marshal(oldObj)
	if err != nil {
		return Unstructured{}, err
	}
	merged, err := applyMergePatch(raw, patch)
	if err != nil {
		return Unstructured{}, err
	}
	var out Unstructured
	if err := json.Unmarshal(merged, &out); err != nil {
		return Unstructured{}, err
	}
	return out, nil
}

func applyRawMergePatch(target json.RawMessage, patch []byte) (json.RawMessage, error) {
	if len(target) == 0 {
		target = []byte(`{}`)
	}
	return applyMergePatch(target, patch)
}

func applyMergePatch(target, patch []byte) ([]byte, error) {
	var targetValue any
	if err := json.Unmarshal(target, &targetValue); err != nil {
		return nil, err
	}
	var patchValue any
	if err := json.Unmarshal(patch, &patchValue); err != nil {
		return nil, err
	}
	merged := mergePatchValue(targetValue, patchValue)
	if merged == nil {
		return nil, nil
	}
	return json.Marshal(merged)
}

func mergePatchValue(target, patch any) any {
	patchMap, ok := patch.(map[string]any)
	if !ok {
		return patch
	}

	targetMap, ok := target.(map[string]any)
	if !ok {
		targetMap = map[string]any{}
	}
	out := make(map[string]any, len(targetMap))
	for key, value := range targetMap {
		out[key] = value
	}
	for key, value := range patchMap {
		if value == nil {
			delete(out, key)
			continue
		}
		out[key] = mergePatchValue(out[key], value)
	}
	return out
}
