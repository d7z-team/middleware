package kv

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemoryRejectsCorruptedStore(t *testing.T) {
	store := filepath.Join(t.TempDir(), "kv.json")
	require.NoError(t, os.WriteFile(store, []byte("{broken"), 0o644))

	_, err := NewMemory(store)
	require.Error(t, err)
}

func TestMemorySyncCreatesNestedStoreAndReloads(t *testing.T) {
	store := filepath.Join(t.TempDir(), "nested", "state", "kv.json")
	kv, err := NewMemory(store)
	require.NoError(t, err)

	require.NoError(t, kv.Put(context.Background(), "key", "value", TTLKeep))
	require.NoError(t, kv.Sync())

	reloaded, err := NewMemory(store)
	require.NoError(t, err)

	value, err := reloaded.Get(context.Background(), "key")
	require.NoError(t, err)
	require.Equal(t, "value", value)
}
