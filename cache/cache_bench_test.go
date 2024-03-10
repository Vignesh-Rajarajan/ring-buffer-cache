package cache

import (
	"fmt"
	"github.com/Vignesh-Rajarajan/ring-buffer-cache/config"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

var message = []byte(`lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum." 
	"lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. 
	"lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.`)

func BenchmarkWriteToCacheWith1Shard(b *testing.B) {
	writeToCache(b, 1, 100*time.Second, b.N)
}

func BenchmarkWriteToCacheWith500Shard(b *testing.B) {
	writeToCache(b, 512, 100*time.Second, b.N)
}

func BenchmarkWriteToCacheWith1000Shard(b *testing.B) {
	writeToCache(b, 1024, 100*time.Second, b.N)
}

func BenchmarkWriteToCacheWithMaxSize(b *testing.B) {
	conf := config.NewConfig(1, 100*time.Second, 100, 256, 1)
	c, _ := NewCache(conf)
	value := blob('a', 1024)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {

		c.Set(fmt.Sprintf("key-%d", i), value)
	}
}

func BenchmarkWriteToCacheWithWithSmallReqWindow(b *testing.B) {
	writeToCache(b, 1024, 100*time.Second, 100)
}

func BenchmarkReadFromCacheWith1Shard(b *testing.B) {
	readFromCache(b, 1024)
}

func writeToCache(b *testing.B, shards int, ttl time.Duration, maxEntriesWindow int) {
	cache, _ := NewCache(&config.Config{
		Shards:           shards,
		TTL:              ttl,
		MaxEntriesWindow: int(math.Max(float64(maxEntriesWindow), 100)),
		MaxEntrySize:     500,
	})
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	b.RunParallel(func(pb *testing.PB) {
		b.ReportAllocs()
		id := r.Intn(1000)
		counter := 0
		for pb.Next() {
			cache.Set(fmt.Sprintf("key-%d-%d", id, counter), message)
			counter++
		}
	})
}

func readFromCache(b *testing.B, shards int) {
	cache, _ := NewCache(&config.Config{
		Shards:           shards,
		TTL:              100 * time.Second,
		MaxEntriesWindow: int(math.Max(float64(b.N), 100)),
		MaxEntrySize:     500,
	})
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		b.ReportAllocs()
		for pb.Next() {
			_, _ = cache.Get(strconv.Itoa(rand.Intn(b.N)))
		}
	})
}
