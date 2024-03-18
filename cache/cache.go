package cache

import (
	"fmt"
	"github.com/Vignesh-Rajarajan/ring-buffer-cache/config"
	"github.com/Vignesh-Rajarajan/ring-buffer-cache/encoding"
	"github.com/Vignesh-Rajarajan/ring-buffer-cache/shard"
	"hash/fnv"
	"log"
	"sync"
)

const (
	minimumEntries = 10
)

type Cache struct {
	shards       []*shard.CacheShard
	ttl          uint64
	clock        clock
	hash         func([]byte) uint64
	config       *config.Config
	maxShardSize uint32
}

type EntryInfo struct {
	Key       string
	Hash      uint64
	Timestamp uint64
}

type KeyIterator chan EntryInfo

func NewCache(config *config.Config) (*Cache, error) {
	return newCache(config, &realClock{}, hash)
}

func newCache(conf *config.Config, clock clock, hash func([]byte) uint64) (*Cache, error) {
	if (conf.Shards & (conf.Shards - 1)) != 0 {
		return nil, fmt.Errorf("shards should be power of 2")
	}
	shards := make([]*shard.CacheShard, conf.Shards)

	for i := 0; i < conf.Shards; i++ {
		logger := log.Default()
		logger.SetPrefix(fmt.Sprintf("cacheShard %d", i))
		shards[i] = shard.NewCacheShard(conf)
	}
	return &Cache{
		shards:       shards,
		ttl:          uint64(conf.TTL.Seconds()),
		clock:        clock,
		hash:         hash,
		maxShardSize: uint32(conf.MaxShardSize()),
		config:       conf,
	}, nil
}

func (c *Cache) Set(key string, value []byte) error {
	hashedKey := c.hash([]byte(key))
	cacheShard := c.getShard(hashedKey)
	cacheShard.Lock.Lock()
	defer cacheShard.Lock.Unlock()

	currTime := c.clock.epoch()

	if prevIndex := cacheShard.Buckets[hashedKey]; prevIndex != 0 {
		if prevEntry, err := cacheShard.Entries.Get(int(prevIndex)); err == nil {
			encoding.ResetKeyFromEntry(prevEntry)
		}
	}

	if frontEntry, err := cacheShard.Entries.Peek(); err == nil {
		c.onEvict(frontEntry, uint64(currTime), cacheShard.RemoveOldestEntry)
	}

	entry := encoding.WrapEntry(currTime, hashedKey, key, value, &cacheShard.EntryBuffer)
	for {
		if index, err := cacheShard.Entries.Enqueue(entry); err == nil {
			cacheShard.Buckets[hashedKey] = uint32(index)
			return nil
		} else if cacheShard.RemoveOldestEntry() != nil {
			return fmt.Errorf("unable to add entry to cache as maxLimit reached %w", err)
		}
	}
}

func (c *Cache) Get(key string) ([]byte, error) {
	hashedKey := c.hash([]byte(key))
	cacheShard := c.getShard(hashedKey)
	cacheShard.Lock.RLock()
	defer cacheShard.Lock.RUnlock()
	index := cacheShard.Buckets[hashedKey]
	if index == 0 {
		return nil, fmt.Errorf("key not found")
	}
	entry, err := cacheShard.Entries.Get(int(index))
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve entry from cache %w", err)
	}
	if encoding.ReadKey(entry) != key {
		return nil, fmt.Errorf("collision detected : both key %q entryKey %q has same hash %x", key, encoding.ReadKey(entry), hashedKey)
	}

	return encoding.ReadEntry(entry), nil
}

func (c *Cache) Keys() KeyIterator {
	ch := make(chan EntryInfo, 1024)
	wg := sync.WaitGroup{}
	wg.Add(len(c.shards))
	for _, cacheShard := range c.shards {
		go func(shard *shard.CacheShard) {
			defer wg.Done()
			shard.Lock.RLock()
			defer shard.Lock.RUnlock()
			for _, index := range shard.Buckets {
				if entry, err := shard.Entries.Get(int(index)); err == nil {
					ch <- EntryInfo{
						Key:       encoding.ReadKey(entry),
						Hash:      encoding.ReadHash(entry),
						Timestamp: uint64(encoding.ReadTimestamp(entry)),
					}
				}
			}

		}(cacheShard)
	}
	return ch
}

func (c *Cache) getShard(hashedKey uint64) *shard.CacheShard {
	return c.shards[hashedKey%uint64(len(c.shards))]
}

func (c *Cache) Reset() {

}

func (c *Cache) onEvict(entry []byte, currentTimestamp uint64, evict func() error) {
	evictEntryTimestamp := uint64(encoding.ReadTimestamp(entry))
	if currentTimestamp-evictEntryTimestamp > c.ttl {
		evict()

	}
}

func hash(b []byte) uint64 {
	h := fnv.New64a()
	h.Write(b)
	return h.Sum64()
}
