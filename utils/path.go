package utils

import (
	"errors"
	"strings"
)

var ErrInvalidPath = errors.New("invalid path")

// NormalizeChild joins developer-provided namespace segments.
// It is intentionally strict: Child APIs must not receive user input.
func NormalizeChild(paths ...string) (string, error) {
	if len(paths) == 0 {
		return "", nil
	}

	segments := make([]string, 0, len(paths))
	for _, raw := range paths {
		raw = strings.ReplaceAll(raw, "\\", "/")
		if raw == "" {
			return "", ErrInvalidPath
		}
		for _, part := range strings.Split(raw, "/") {
			if invalidPathPart(part) {
				return "", ErrInvalidPath
			}
			segments = append(segments, part)
		}
	}
	return strings.Join(segments, "/"), nil
}

func MustChild(paths ...string) string {
	child, err := NormalizeChild(paths...)
	if err != nil {
		panic(err)
	}
	return child
}

// NormalizePath normalizes runtime file paths. Absolute-looking names are
// treated as rooted virtual paths, but traversal components are rejected.
func NormalizePath(name string) (string, error) {
	name = strings.ReplaceAll(name, "\\", "/")
	if name == "" || name == "." || name == "/" {
		return "", nil
	}

	name = strings.Trim(name, "/")
	if name == "" {
		return "", nil
	}

	segments := make([]string, 0, strings.Count(name, "/")+1)
	for _, part := range strings.Split(name, "/") {
		if invalidPathPart(part) {
			return "", ErrInvalidPath
		}
		segments = append(segments, part)
	}
	return strings.Join(segments, "/"), nil
}

func invalidPathPart(part string) bool {
	return part == "" || part == "." || part == ".."
}
