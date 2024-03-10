package cache

import (
	"fmt"
	"github.com/Vignesh-Rajarajan/ring-buffer-cache/circular_queue"
	"github.com/Vignesh-Rajarajan/ring-buffer-cache/config"
	"github.com/Vignesh-Rajarajan/ring-buffer-cache/encoding"
	"hash/fnv"
	"log"
	"math"
	"sync"
)

const (
	minimumEntries = 10
)

type Cache struct {
	shards []*cacheShard
	ttl    uint64
	clock  clock
	hash   func([]byte) uint64
}

type cacheShard struct {
	buckets map[uint64]uint32
	entries circular_queue.ByteStorageQueue
	lock    sync.RWMutex
	logger  *log.Logger
}

func NewCache(config *config.Config) *Cache {
	return newCache(config, &realClock{}, hash)
}

func newCache(config *config.Config, clock clock, hash func([]byte) uint64) *Cache {
	shards := make([]*cacheShard, config.Shards)
	shardSize := int(math.Max(float64(config.MaxEntriesWindow/config.Shards), minimumEntries))
	for i := 0; i < config.Shards; i++ {
		logger := log.Default()
		logger.SetPrefix(fmt.Sprintf("cacheShard %d", i))
		shards[i] = &cacheShard{
			buckets: make(map[uint64]uint32, uint32(shardSize)),
			entries: *circular_queue.NewByteStorageQueue(shardSize*config.MaxEntrySize, logger),
			logger:  logger,
		}
	}
	return &Cache{
		shards: shards,
		ttl:    uint64(config.TTL.Seconds()),
		clock:  clock,
		hash:   hash,
	}
}

func (c *Cache) Set(key string, value []byte) {
	hashedKey := c.hash([]byte(key))
	shard := c.getShard(hashedKey)
	shard.lock.Lock()
	defer shard.lock.Unlock()

	currTime := c.clock.epoch()

	if prevIndex := shard.buckets[hashedKey]; prevIndex != 0 {
		if prevEntry, err := shard.entries.Get(int(prevIndex)); err == nil {
			encoding.ResetKeyFromEntry(prevEntry)
		}
	}

	if frontEntry, err := shard.entries.Peek(); err == nil {
		c.onEvict(frontEntry, uint64(currTime), func() {
			shard.entries.Dequeue()
			hash := encoding.ReadHash(frontEntry)
			delete(shard.buckets, hash)
		})
	}

	entry := encoding.WrapEntry(currTime, hashedKey, key, value)
	index := shard.entries.Enqueue(entry)
	shard.buckets[hashedKey] = uint32(index)
}

func (c *Cache) Get(key string) ([]byte, error) {
	hashedKey := c.hash([]byte(key))
	shard := c.getShard(hashedKey)
	shard.lock.RLock()
	defer shard.lock.RUnlock()
	index := shard.buckets[hashedKey]
	if index == 0 {
		return nil, fmt.Errorf("key not found")
	}
	entry, err := shard.entries.Get(int(index))
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve entry from cache %w", err)
	}
	if encoding.ReadKey(entry) != key {
		return nil, fmt.Errorf("collision detected : both key %q entryKey %q has same hash %x", key, encoding.ReadKey(entry), hashedKey)
	}

	return encoding.ReadEntry(entry), nil
}

func (c *Cache) getShard(hashedKey uint64) *cacheShard {
	return c.shards[hashedKey%uint64(len(c.shards))]
}

func (c *Cache) onEvict(entry []byte, currentTimestamp uint64, evict func()) {
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
