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
	shards       []*cacheShard
	ttl          uint64
	clock        clock
	hash         func([]byte) uint64
	config       *config.Config
	maxShardSize uint32
}

type cacheShard struct {
	buckets          map[uint64]uint32
	entries          circular_queue.ByteStorageQueue
	lock             sync.RWMutex
	logger           *log.Logger
	entryBuffer      []byte
	onRemoveCallback func(string, []byte)
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

func newCache(config *config.Config, clock clock, hash func([]byte) uint64) (*Cache, error) {
	if (config.Shards & (config.Shards - 1)) != 0 {
		return nil, fmt.Errorf("shards should be power of 2")
	}
	maxShardSize := 0
	if config.HardMaxCacheSize > 0 {
		maxShardSize = (config.HardMaxCacheSize / config.Shards) * 1024 * 1024
	}
	shards := make([]*cacheShard, config.Shards)

	shardSize := int(math.Max(float64(config.MaxEntriesWindow/config.Shards), minimumEntries))
	for i := 0; i < config.Shards; i++ {
		logger := log.Default()
		logger.SetPrefix(fmt.Sprintf("cacheShard %d", i))
		shards[i] = &cacheShard{
			buckets:          make(map[uint64]uint32, uint32(shardSize)),
			entries:          *circular_queue.NewByteStorageQueue(shardSize*config.MaxEntrySize, maxShardSize, logger),
			logger:           logger,
			entryBuffer:      make([]byte, config.MaxEntrySize+encoding.HeaderSizeInBytes),
			onRemoveCallback: extractOnRemoveCallBack(*config),
		}
	}
	return &Cache{
		shards: shards,
		ttl:    uint64(config.TTL.Seconds()),
		clock:  clock,
		hash:   hash,
	}, nil
}

func (c *Cache) Set(key string, value []byte) error {
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
		c.onEvict(frontEntry, uint64(currTime), shard.removeOldestEntry)
	}

	entry := encoding.WrapEntry(currTime, hashedKey, key, value, &shard.entryBuffer)
	for {
		if index, err := shard.entries.Enqueue(entry); err == nil {
			shard.buckets[hashedKey] = uint32(index)
			return nil
		} else if shard.removeOldestEntry() != nil {
			return fmt.Errorf("unable to add entry to cache as maxLimit reached %w", err)
		}
	}
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

func (c *Cache) Keys() KeyIterator {
	ch := make(chan EntryInfo, 1024)
	wg := sync.WaitGroup{}
	wg.Add(len(c.shards))
	for _, shard := range c.shards {
		go func(shard *cacheShard) {
			defer wg.Done()
			shard.lock.RLock()
			defer shard.lock.RUnlock()
			for _, index := range shard.buckets {
				if entry, err := shard.entries.Get(int(index)); err == nil {
					ch <- EntryInfo{
						Key:       encoding.ReadKey(entry),
						Hash:      encoding.ReadHash(entry),
						Timestamp: uint64(encoding.ReadTimestamp(entry)),
					}
				}
			}

		}(shard)
	}
	return ch
}

func (c *Cache) getShard(hashedKey uint64) *cacheShard {
	return c.shards[hashedKey%uint64(len(c.shards))]
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

func (cs *cacheShard) removeOldestEntry() error {
	oldest, err := cs.entries.Dequeue()
	if err == nil {
		hash := encoding.ReadHash(oldest)
		delete(cs.buckets, hash)
		cs.onRemoveCallback(encoding.ReadKey(oldest), encoding.ReadEntry(oldest))
		return nil
	}
	return err
}

func extractOnRemoveCallBack(c config.Config) func(string, []byte) {
	if c.OnRemoveCallback != nil {
		return c.OnRemoveCallback
	}
	return func(s string, bytes []byte) {}
}
