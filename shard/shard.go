package shard

import (
	"github.com/Vignesh-Rajarajan/ring-buffer-cache/circular_queue"
	"github.com/Vignesh-Rajarajan/ring-buffer-cache/config"
	"github.com/Vignesh-Rajarajan/ring-buffer-cache/encoding"
	"log"
	"sync"
)

type CacheShard struct {
	Buckets          map[uint64]uint32
	Entries          circular_queue.ByteStorageQueue
	Lock             sync.RWMutex
	Logger           *log.Logger
	EntryBuffer      []byte
	OnRemoveCallback func(string, []byte)
}

func (cs *CacheShard) RemoveOldestEntry() error {
	oldest, err := cs.Entries.Dequeue()
	if err == nil {
		hash := encoding.ReadHash(oldest)
		delete(cs.Buckets, hash)
		cs.OnRemoveCallback(encoding.ReadKey(oldest), encoding.ReadEntry(oldest))
		return nil
	}
	return err
}

func (cs *CacheShard) Reset(conf *config.Config) {
	cs.Lock.Lock()
	defer cs.Lock.Unlock()
	cs.Buckets = make(map[uint64]uint32, conf.InitialShardSize())
	cs.EntryBuffer = make([]byte, conf.MaxEntriesWindow+encoding.HeaderSizeInBytes)
	cs.Entries.Reset()
}

func (cs *CacheShard) Len() int {
	return len(cs.Buckets)
}

func NewCacheShard(conf *config.Config) *CacheShard {
	return &CacheShard{
		Buckets:          make(map[uint64]uint32),
		Entries:          *circular_queue.NewByteStorageQueue(conf.InitialShardSize()*conf.MaxEntrySize, conf.HardMaxCacheSize, log.Default()),
		Logger:           log.Default(),
		EntryBuffer:      make([]byte, conf.MaxEntrySize+encoding.HeaderSizeInBytes),
		OnRemoveCallback: extractOnRemoveCallBack(conf),
	}
}

func extractOnRemoveCallBack(c *config.Config) func(string, []byte) {
	if c.OnRemoveCallback != nil {
		return c.OnRemoveCallback
	}
	return func(s string, bytes []byte) {}
}
