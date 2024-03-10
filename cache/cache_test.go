package cache

import (
	"github.com/Vignesh-Rajarajan/ring-buffer-cache/config"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestWriteAndGetCache(t *testing.T) {
	t.Parallel()
	conf := config.NewConfig(10, 5*time.Second, 10, 256)
	c := NewCache(conf)

	value := []byte("hello")

	c.Set("key", value)
	cachedValue, err := c.Get("key")
	assert.NoError(t, err)
	assert.Equal(t, value, cachedValue)
}

func TestNotFound(t *testing.T) {
	t.Parallel()
	conf := config.NewConfig(10, 5*time.Second, 10, 256)
	c := NewCache(conf)

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
	c := newCache(config.NewConfig(1, time.Second, 10, 256), &cl, hash)
	c.Set("key", []byte("value"))
	cl.set(6)
	c.Set("key1", []byte("value1"))
	_, err := c.Get("key")
	assert.EqualError(t, err, "key not found")
}

func TestKeyUpdate(t *testing.T) {
	t.Parallel()
	cl := mockClock{value: 0}
	c := newCache(config.NewConfig(1, 6*time.Second, 1, 256), &cl, hash)
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
	conf := config.NewConfig(10, 5*time.Second, 10, 256)
	c := NewCache(conf)
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
