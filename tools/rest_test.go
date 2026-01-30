package tools

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.d7z.net/middleware/kv"
)

type User struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func TestID2KV(t *testing.T) {
	memory, err := kv.NewMemory("")
	assert.NoError(t, err)

	id2kv := NewID2KV[User](memory, "/users/")

	user1 := User{Name: "Alice", Age: 30}

	// Put
	err = id2kv.Put(t.Context(), "1", user1)
	assert.NoError(t, err)

	// Get
	got, err := id2kv.Get(t.Context(), "1")
	assert.NoError(t, err)
	assert.Equal(t, user1, got)

	// PutIfNotExists
	ok, err := id2kv.PutIfNotExists(t.Context(), "1", User{Name: "Bob", Age: 25})
	assert.NoError(t, err)
	assert.False(t, ok) // Already exists

	got, err = id2kv.Get(t.Context(), "1")
	assert.NoError(t, err)
	assert.Equal(t, user1, got) // Should still be Alice

	ok, err = id2kv.PutIfNotExists(t.Context(), "2", User{Name: "Charlie", Age: 35})
	assert.NoError(t, err)
	assert.True(t, ok)

	// List
	list, err := id2kv.List(t.Context(), 0, 10)
	assert.NoError(t, err)
	assert.Len(t, list, 2)
	assert.Equal(t, user1, list["1"])

	// Delete
	ok, err = id2kv.Delete(t.Context(), "1")
	assert.NoError(t, err)
	assert.True(t, ok)

	_, err = id2kv.Get(t.Context(), "1")
	assert.Error(t, err)
}
