package config

import (
	"math"
	"time"
)

const (
	minimumEntries = 10
)

type Config struct {
	Shards int
	TTL    time.Duration
	// MaxEntriesWindow is the maximum number of entries that can be stored in a shard initially
	MaxEntriesWindow int
	MaxEntrySize     int
	HardMaxCacheSize int
	OnRemoveCallback func(string, []byte)
}

func NewConfig(shards int, ttl time.Duration, maxEntriesWindow int, maxEntrySize, HardMaxCacheSize int) *Config {
	return &Config{
		Shards:           shards,
		TTL:              ttl,
		MaxEntriesWindow: maxEntriesWindow,
		MaxEntrySize:     maxEntrySize,
		HardMaxCacheSize: HardMaxCacheSize,
	}
}

func (c *Config) InitialShardSize() int {
	maxShardSize := 0
	if c.HardMaxCacheSize > 0 {
		maxShardSize = int(math.Max(float64(c.MaxEntriesWindow/c.Shards), minimumEntries))

	}
	return maxShardSize
}

func (c *Config) MaxShardSize() int {
	return (c.HardMaxCacheSize / c.Shards) * 1024 * 1024
}
