package cache

import (
	"os"
	"runtime/debug"
	"strconv"
	"strings"
)

const (
	fallbackMemoryCacheMaxBytes = 512 << 20
	maxInt64Uint                = uint64(1<<63 - 1)
	unlimitedMemoryThreshold    = uint64(1 << 60)
)

func defaultMemoryCacheMaxBytes() uint64 {
	if limit := debug.SetMemoryLimit(-1); limit > 0 && limit < int64(maxInt64Uint) {
		return uint64(limit) / 2
	}
	if limit := linuxCgroupMemoryLimit(); limit > 0 {
		return limit / 2
	}
	if total := linuxMemTotal(); total > 0 {
		return total / 2
	}
	return fallbackMemoryCacheMaxBytes
}

func linuxCgroupMemoryLimit() uint64 {
	for _, path := range []string{
		"/sys/fs/cgroup/memory.max",
		"/sys/fs/cgroup/memory/memory.limit_in_bytes",
	} {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		raw := strings.TrimSpace(string(data))
		if raw == "" || raw == "max" {
			continue
		}
		limit, err := strconv.ParseUint(raw, 10, 64)
		if err == nil && limit > 0 && limit < unlimitedMemoryThreshold {
			return limit
		}
	}
	return 0
}

func linuxMemTotal() uint64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		kib, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0
		}
		return kib * 1024
	}
	return 0
}
