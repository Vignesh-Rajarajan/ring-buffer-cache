package config

import "time"

type Config struct {
	Shards           int
	TTL              time.Duration
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
