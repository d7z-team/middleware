package storage

import (
	"io"
	"strings"
	"testing"
)

func TestStorageInterface(t *testing.T) {
	// 测试用例
	testCases := []struct {
		name       string
		url        string
		skip       bool
		skipReason string
	}{
		{
			name: "memory_storage",
			url:  "memory://",
		},
		{
			name: "file_storage",
			url:  "file:///tmp/test_storage",
		},
		{
			name:       "s3_storage",
			url:        "s3://minio_admin:minio_admin@127.0.0.1:9000/test-bucket",
			skipReason: "需要运行的 MinIO 服务",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skip(tc.skipReason)
			}

			storage, err := NewStorageFromURL(tc.url)
			if err != nil {
				t.Fatalf("创建存储失败: %v", err)
			}
			defer storage.Close()

			// 运行通用的存储接口测试
			runStorageTests(t, storage)
		})
	}
}

func runStorageTests(t *testing.T, storage Storage) {
	testPath := "test-file.txt"
	testContent := "Hello, Storage Interface Test!"

	// 测试 1: 检查文件是否存在（应该不存在）
	t.Run("Exists_NonExistentFile", func(t *testing.T) {
		if storage.Exists(testPath) {
			t.Error("文件不应该存在")
		}
	})

	// 测试 2: 推送文件
	t.Run("Push_File", func(t *testing.T) {
		reader := strings.NewReader(testContent)
		err := storage.Push(testPath, reader)
		if err != nil {
			t.Fatalf("推送文件失败: %v", err)
		}
	})

	// 测试 3: 检查文件是否存在（现在应该存在）
	t.Run("Exists_AfterPush", func(t *testing.T) {
		if !storage.Exists(testPath) {
			t.Error("文件应该存在")
		}
	})

	// 测试 4: 拉取文件并验证内容
	t.Run("Pull_And_Verify", func(t *testing.T) {
		reader, err := storage.Pull(testPath)
		if err != nil {
			t.Fatalf("拉取文件失败: %v", err)
		}
		defer reader.Close()

		content, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("读取文件内容失败: %v", err)
		}

		if string(content) != testContent {
			t.Errorf("文件内容不匹配，期望: %s, 实际: %s", testContent, string(content))
		}
	})

	// 测试 5: 列出文件
	t.Run("List_Files", func(t *testing.T) {
		files, err := storage.List("")
		if err != nil {
			t.Fatalf("列出文件失败: %v", err)
		}

		found := false
		for _, file := range files {
			if file == testPath || strings.Contains(file, testPath) {
				found = true
				break
			}
		}

		if !found {
			t.Error("应该在文件列表中找到测试文件")
		}
	})

	// 测试 6: 删除文件
	t.Run("Remove_File", func(t *testing.T) {
		err := storage.Remove(testPath)
		if err != nil {
			t.Fatalf("删除文件失败: %v", err)
		}

		// 验证文件已被删除
		if storage.Exists(testPath) {
			t.Error("文件应该已被删除")
		}
	})

	// 测试 7: 测试嵌套目录
	t.Run("Nested_Directory", func(t *testing.T) {
		nestedPath := "subdir/nested-file.txt"
		nestedContent := "Nested file content"

		// 推送嵌套文件
		err := storage.Push(nestedPath, strings.NewReader(nestedContent))
		if err != nil {
			t.Fatalf("推送嵌套文件失败: %v", err)
		}

		// 验证嵌套文件存在
		if !storage.Exists(nestedPath) {
			t.Error("嵌套文件应该存在")
		}

		// 拉取并验证内容
		reader, err := storage.Pull(nestedPath)
		if err != nil {
			t.Fatalf("拉取嵌套文件失败: %v", err)
		}
		defer reader.Close()

		content, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("读取嵌套文件内容失败: %v", err)
		}

		if string(content) != nestedContent {
			t.Errorf("嵌套文件内容不匹配，期望: %s, 实际: %s", nestedContent, string(content))
		}

		// 清理
		err = storage.Remove(nestedPath)
		if err != nil {
			t.Fatalf("删除嵌套文件失败: %v", err)
		}
	})
}

// 专门测试 S3 存储（需要 MinIO 服务运行）
func TestS3StorageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过集成测试")
	}

	s3URL := "s3://minio_admin:minio_admin@127.0.0.1:9000/test-bucket"
	storage, err := NewStorageFromURL(s3URL)
	if err != nil {
		t.Skipf("无法连接到 S3 存储，跳过测试: %v", err)
	}
	defer storage.Close()

	// 运行完整的存储测试
	runStorageTests(t, storage)
}

// 测试错误情况
func TestStorageErrorCases(t *testing.T) {
	tests := []struct {
		name        string
		url         string
		expectError bool
	}{
		{
			name:        "invalid_url",
			url:         "invalid://test",
			expectError: true,
		},
		{
			name:        "unsupported_protocol",
			url:         "ftp://example.com",
			expectError: true,
		},
		{
			name:        "valid_memory",
			url:         "memory://",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage, err := NewStorageFromURL(tt.url)

			if tt.expectError {
				if err == nil {
					t.Error("期望出现错误，但得到了 nil")
				}
				if storage != nil {
					storage.Close()
					t.Error("出现错误时存储实例应该为 nil")
				}
			} else {
				if err != nil {
					t.Errorf("不期望出现错误，但得到: %v", err)
				}
				if storage == nil {
					t.Error("存储实例不应该为 nil")
				} else {
					storage.Close()
				}
			}
		})
	}
}

// 基准测试
func BenchmarkStorageOperations(b *testing.B) {
	storage, err := NewStorageFromURL("memory://")
	if err != nil {
		b.Fatalf("创建存储失败: %v", err)
	}
	defer storage.Close()

	testContent := "Benchmark test content"

	b.Run("Push", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			path := string(rune(i)) // 简单的路径生成
			reader := strings.NewReader(testContent)
			err := storage.Push(path, reader)
			if err != nil {
				b.Fatalf("推送失败: %v", err)
			}
		}
	})

	b.Run("Exists", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			path := string(rune(i % 100)) // 限制路径数量
			_ = storage.Exists(path)
		}
	})
}
