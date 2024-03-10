package encoding

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestEncode(t *testing.T) {
	now := uint64(time.Now().Unix())
	hash := uint64(42)
	key := "key"
	value := []byte("value")
	buffer := make([]byte, 100)
	blob := WrapEntry(int64(now), hash, key, value, &buffer)

	assert.Equal(t, key, ReadKey(blob))
	assert.Equal(t, value, ReadEntry(blob))
	assert.Equal(t, int64(now), ReadTimestamp(blob))
	assert.Equal(t, hash, ReadHash(blob))
	assert.Equal(t, 100, cap(buffer))
}

func TestAllocateBiggerBuffer(t *testing.T) {
	now := uint64(time.Now().Unix())
	hash := uint64(42)
	key := "key"
	value := []byte("value")
	buffer := make([]byte, 1)

	blob := WrapEntry(int64(now), hash, key, value, &buffer)
	assert.Equal(t, key, ReadKey(blob))
	assert.Equal(t, value, ReadEntry(blob))
	assert.Equal(t, int64(now), ReadTimestamp(blob))
	assert.Equal(t, hash, ReadHash(blob))
	assert.Equal(t, 8+HeaderSizeInBytes, cap(buffer)) // keyLen+valueLen+HeaderSizeInBytes
}
