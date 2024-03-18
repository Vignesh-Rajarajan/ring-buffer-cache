package cache

import (
	"github.com/Vignesh-Rajarajan/ring-buffer-cache/config"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestWriteAndGetCache(t *testing.T) {
	t.Parallel()
	conf := config.NewConfig(16, 5*time.Second, 10, 256, 1)
	c, _ := NewCache(conf)

	value := []byte("hello")

	c.Set("key", value)
	cachedValue, err := c.Get("key")
	assert.NoError(t, err)
	assert.Equal(t, value, cachedValue)
}

func TestNotFound(t *testing.T) {
	t.Parallel()
	conf := config.NewConfig(16, 5*time.Second, 10, 256, 1)
	c, _ := NewCache(conf)

	_, err := c.Get("key")
	assert.EqualError(t, err, "key not found")
}

type mockClock struct {
	value int64
}

func (mc *mockClock) epoch() int64 {
	return mc.value
}

func (mc *mockClock) set(val int64) {
	mc.value = val
}

func TestKeyEvictionWithTTL(t *testing.T) {
	t.Parallel()
	cl := mockClock{value: 0}
	c, _ := newCache(config.NewConfig(1, time.Second, 10, 256, 1), &cl, hash)
	c.Set("key", []byte("value"))
	cl.set(6)
	c.Set("key1", []byte("value1"))
	_, err := c.Get("key")
	assert.EqualError(t, err, "key not found")
}

func TestKeyUpdate(t *testing.T) {
	t.Parallel()
	cl := mockClock{value: 0}
	c, _ := newCache(config.NewConfig(1, 6*time.Second, 1, 256, 1), &cl, hash)
	c.Set("key", []byte("value"))
	cl.set(5)
	c.Set("key", []byte("value1"))
	cl.set(7)
	c.Set("key2", []byte("value2"))
	val, err := c.Get("key")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value1"), val)
}

func hashStub(_ []byte) uint64 {
	return 5
}

func TestHashCollision(t *testing.T) {
	t.Parallel()
	conf := config.NewConfig(16, 5*time.Second, 10, 256, 1)
	c, _ := NewCache(conf)
	c.hash = hashStub

	c.Set("key", []byte("value"))
	val, err := c.Get("key")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value"), val)

	c.Set("test", []byte("value treu"))
	val, err = c.Get("test")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value treu"), val)

	val, err = c.Get("key")
	assert.EqualError(t, err, "collision detected : both key \"key\" entryKey \"test\" has same hash 5")
}

func TestEntryBiggerThanMaxCacheSize(t *testing.T) {
	t.Parallel()
	conf := config.NewConfig(1, 5*time.Second, 10, 256, 1)
	conf.HardMaxCacheSize = 1
	c, _ := NewCache(conf)
	c.hash = hashStub

	err := c.Set("key", blob('a', 1024*1025))
	assert.Error(t, err)
}

func TestOldestEntryRemoval(t *testing.T) {
	t.Parallel()
	conf := config.NewConfig(1, 10*time.Second, 1, 1, 1)
	c, _ := NewCache(conf)

	c.Set("key1", blob('a', 1024*400))
	c.Set("key2", blob('b', 1024*400))
	c.Set("key3", blob('c', 1024*800))

	_, err := c.Get("key1")
	assert.EqualError(t, err, "key not found")
	_, err = c.Get("key2")
	assert.EqualError(t, err, "key not found")

}

func TestOnRemoveCallBack(t *testing.T) {
	t.Parallel()
	conf := config.NewConfig(1, time.Second, 1, 256, 1)
	onRemoveInvoked := false
	conf.OnRemoveCallback = func(key string, value []byte) {
		onRemoveInvoked = true
		assert.Equal(t, "key", key)
		assert.Equal(t, []byte("value"), value)
	}

	c, _ := NewCache(conf)
	clock := &mockClock{value: 0}
	c.clock = clock

	c.Set("key", []byte("value"))
	clock.set(5)
	c.Set("key2", []byte("value2"))
	assert.True(t, onRemoveInvoked)
}

func blob(b byte, length int) []byte {
	buff := make([]byte, length)
	for idx := range buff {
		buff[idx] = b
	}
	return buff
}
