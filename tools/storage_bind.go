package tools

import (
	"os"

	"gopkg.d7z.net/middleware/storage"
)

type StorageBind struct {
	storage.Storage
}

func NewStorageBind(s storage.Storage) StorageBind {
	return StorageBind{
		Storage: s,
	}
}

func (e StorageBind) PushFile(path, file string) error {
	f, err := os.OpenFile(file, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return err
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)
	return e.Push(path, f)
}
