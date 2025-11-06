package cache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMemoryCache_NewMemoryCache(t *testing.T) {
	tests := []struct {
		name        string
		config      MemoryCacheConfig
		wantErr     bool
		errContains string
	}{
		{
			name: "valid config",
			config: MemoryCacheConfig{
				MaxCapacity: 100,
				CleanupInt:  time.Minute,
			},
			wantErr: false,
		},
		{
			name: "zero capacity",
			config: MemoryCacheConfig{
				MaxCapacity: 0,
			},
			wantErr:     true,
			errContains: "invalid cache capacity",
		},
		{
			name: "negative capacity",
			config: MemoryCacheConfig{
				MaxCapacity: -1,
			},
			wantErr:     true,
			errContains: "invalid cache capacity",
		},
		{
			name: "default cleanup interval",
			config: MemoryCacheConfig{
				MaxCapacity: 100,
				CleanupInt:  0,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache, err := NewMemoryCache(tt.config)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error but got none")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error should contain %q, got %q", tt.errContains, err.Error())
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if cache == nil {
				t.Error("cache should not be nil")
			}

			defer cache.Close()
		})
	}
}

func TestMemoryCache_PutAndGet(t *testing.T) {
	config := MemoryCacheConfig{
		MaxCapacity: 10,
		CleanupInt:  time.Minute,
	}
	cache, err := NewMemoryCache(config)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	ctx := context.Background()
	key := "test-key"
	data := "test data"

	err = cache.Put(ctx, key, strings.NewReader(data), TTLKeep)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	content, err := cache.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if content.Length != len(data) {
		t.Errorf("expected length %d, got %d", len(data), content.Length)
	}

	// Read and verify data
	readData, err := io.ReadAll(content.ReadSeekCloser)
	if err != nil {
		t.Fatalf("failed to read content: %v", err)
	}

	if string(readData) != data {
		t.Errorf("expected data %q, got %q", data, string(readData))
	}
}

func TestMemoryCache_Get_Miss(t *testing.T) {
	config := MemoryCacheConfig{
		MaxCapacity: 10,
		CleanupInt:  time.Minute,
	}
	cache, err := NewMemoryCache(config)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	ctx := context.Background()
	_, err = cache.Get(ctx, "non-existent-key")
	if !errors.Is(err, ErrCacheMiss) {
		t.Errorf("expected ErrCacheMiss, got %v", err)
	}
}

func TestMemoryCache_Put_InvalidTTL(t *testing.T) {
	config := MemoryCacheConfig{
		MaxCapacity: 10,
		CleanupInt:  time.Minute,
	}
	cache, err := NewMemoryCache(config)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	ctx := context.Background()

	tests := []struct {
		name string
		ttl  time.Duration
	}{
		{"zero TTL", 0},
		{"negative TTL", -time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cache.Put(ctx, "key", strings.NewReader("data"), tt.ttl)
			if !errors.Is(err, ErrInvalidTTL) {
				t.Errorf("expected ErrInvalidTTL, got %v", err)
			}
		})
	}
}

func TestMemoryCache_Put_ReadError(t *testing.T) {
	config := MemoryCacheConfig{
		MaxCapacity: 10,
		CleanupInt:  time.Minute,
	}
	cache, err := NewMemoryCache(config)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	ctx := context.Background()

	// Create a reader that will return an error
	errorReader := &errorReader{err: errors.New("read error")}

	err = cache.Put(ctx, "key", errorReader, TTLKeep)
	if err == nil {
		t.Error("expected error but got none")
	}
}

type errorReader struct {
	err error
}

func (r *errorReader) Read(p []byte) (n int, err error) {
	return 0, r.err
}

func TestMemoryCache_Delete(t *testing.T) {
	config := MemoryCacheConfig{
		MaxCapacity: 10,
		CleanupInt:  time.Minute,
	}
	cache, err := NewMemoryCache(config)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	ctx := context.Background()
	key := "test-key"

	// Put then delete
	err = cache.Put(ctx, key, strings.NewReader("data"), TTLKeep)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	err = cache.Delete(ctx, key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deletion
	_, err = cache.Get(ctx, key)
	if !errors.Is(err, ErrCacheMiss) {
		t.Errorf("expected ErrCacheMiss after deletion, got %v", err)
	}
}

func TestMemoryCache_Expiration(t *testing.T) {
	config := MemoryCacheConfig{
		MaxCapacity: 10,
		CleanupInt:  100 * time.Millisecond, // Short cleanup interval for testing
	}
	cache, err := NewMemoryCache(config)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	ctx := context.Background()
	key := "expiring-key"

	// Put with very short TTL
	err = cache.Put(ctx, key, strings.NewReader("data"), 50*time.Millisecond)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Should be available immediately
	_, err = cache.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed immediately after put: %v", err)
	}

	// Wait for expiration
	time.Sleep(100 * time.Millisecond)

	// Should be expired
	_, err = cache.Get(ctx, key)
	if !errors.Is(err, ErrCacheMiss) {
		t.Errorf("expected ErrCacheMiss after expiration, got %v", err)
	}
}

func TestMemoryCache_LRU_Eviction(t *testing.T) {
	config := MemoryCacheConfig{
		MaxCapacity: 2, // Small capacity to test eviction
		CleanupInt:  time.Minute,
	}
	cache, err := NewMemoryCache(config)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	ctx := context.Background()

	// Fill cache to capacity
	err = cache.Put(ctx, "key1", strings.NewReader("data1"), TTLKeep)
	if err != nil {
		t.Fatalf("Put key1 failed: %v", err)
	}

	err = cache.Put(ctx, "key2", strings.NewReader("data2"), TTLKeep)
	if err != nil {
		t.Fatalf("Put key2 failed: %v", err)
	}

	// Add third item, should trigger eviction
	err = cache.Put(ctx, "key3", strings.NewReader("data3"), TTLKeep)
	if err != nil {
		t.Fatalf("Put key3 failed: %v", err)
	}

	// One of the first two keys should be evicted
	foundCount := 0
	if _, err := cache.Get(ctx, "key1"); err == nil {
		foundCount++
	}
	if _, err := cache.Get(ctx, "key2"); err == nil {
		foundCount++
	}
	if _, err := cache.Get(ctx, "key3"); err == nil {
		foundCount++
	}

	if foundCount != 2 {
		t.Errorf("expected 2 items in cache, found %d", foundCount)
	}
}

func TestMemoryCache_Close(t *testing.T) {
	config := MemoryCacheConfig{
		MaxCapacity: 10,
		CleanupInt:  100 * time.Millisecond,
	}
	cache, err := NewMemoryCache(config)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	// Close should work without error
	err = cache.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Double close should be safe (idempotent)
	err = cache.Close()
	if err != nil {
		t.Errorf("Second Close failed: %v", err)
	}
}

func TestMemoryCache_ConcurrentAccess(t *testing.T) {
	config := MemoryCacheConfig{
		MaxCapacity: 100,
		CleanupInt:  time.Minute,
	}
	cache, err := NewMemoryCache(config)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	ctx := context.Background()
	numGoroutines := 10
	operationsPerGoroutine := 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				key := string(rune('A' + id))
				data := string(rune('a' + j))

				_ = cache.Put(ctx, key, strings.NewReader(data), TTLKeep)
				_, _ = cache.Get(ctx, key)
				_ = cache.Delete(ctx, key)
			}
		}(i)
	}

	wg.Wait()
}

func TestNopCloser(t *testing.T) {
	data := []byte("test data")
	reader := bytes.NewReader(data)
	nopCloser := NopCloser{reader}

	// Test Read
	buf := make([]byte, len(data))
	n, err := nopCloser.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("expected to read %d bytes, got %d", len(data), n)
	}

	// Test Seek
	pos, err := nopCloser.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek failed: %v", err)
	}
	if pos != 0 {
		t.Errorf("expected position 0, got %d", pos)
	}

	// Test Close (should not error)
	err = nopCloser.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestCacheContent(t *testing.T) {
	data := []byte("test data")
	reader := bytes.NewReader(data)
	nopCloser := NopCloser{reader}
	now := time.Now()

	content := &CacheContent{
		ReadSeekCloser: nopCloser,
		Length:         len(data),
		LastModified:   now,
	}

	if content.Length != len(data) {
		t.Errorf("expected length %d, got %d", len(data), content.Length)
	}

	if !content.LastModified.Equal(now) {
		t.Error("LastModified time mismatch")
	}

	// Test that the content can be read
	readData, err := io.ReadAll(content.ReadSeekCloser)
	if err != nil {
		t.Fatalf("failed to read content: %v", err)
	}

	if string(readData) != string(data) {
		t.Errorf("expected data %q, got %q", string(data), string(readData))
	}
}
