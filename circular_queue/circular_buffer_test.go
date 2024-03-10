package circular_queue

import (
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

var logger = log.New(os.Stdout, "test: ", log.LstdFlags)

func TestPushAndPop(t *testing.T) {
	t.Parallel()
	q := NewByteStorageQueue(10, 0, logger)
	data := []byte("hello")
	idx, err := q.Enqueue(data)
	assert.NoError(t, err)
	if idx != 1 {
		t.Errorf("expected 1, got %d", idx)
	}
	data, err = q.Dequeue()
	if err != nil {
		t.Errorf("expected nil, got %s", err)
	}
	if string(data) != "hello" {
		t.Errorf("expected hello, got %s", string(data))
	}
}

func TestPeek(t *testing.T) {
	t.Parallel()
	q := NewByteStorageQueue(10, 0, logger)
	data := []byte("hello")
	q.Enqueue(data)
	data, _ = q.Peek()
	if string(data) != "hello" {
		t.Errorf("expected hello, got %s", string(data))
	}
}

func TestAvailableSpace(t *testing.T) {
	t.Parallel()
	q := NewByteStorageQueue(100, 0, logger)
	q.Enqueue(blob('a', 70))
	q.Enqueue(blob('b', 20))
	_, _ = q.Dequeue()
	q.Enqueue(blob('c', 20))

	assert.Equal(t, 100, q.Capacity())
	val, _ := q.Dequeue()
	assert.Equal(t, blob('b', 20), val)
}

func TestAllocateSpace(t *testing.T) {
	t.Parallel()
	q := NewByteStorageQueue(10, 0, logger)
	q.Enqueue([]byte("hello"))
	q.Enqueue([]byte("hello1"))

	assert.Equal(t, 20, q.Capacity())
}

func TestAdditionalSpaceForBiggerData(t *testing.T) {
	t.Parallel()
	q := NewByteStorageQueue(11, 0, logger)
	q.Enqueue(make([]byte, 100))
	res, _ := q.Dequeue()
	assert.Equal(t, make([]byte, 100), res)
	assert.Equal(t, 230, q.Capacity())

	q = NewByteStorageQueue(25, 0, logger)
	q.Enqueue(blob('a', 3)) // header+entry+leftMargin = 8 bytes
	q.Enqueue(blob('b', 6)) // header+entry = 10 bytes
	q.Dequeue()             // space freed = 7 bytes
	q.Enqueue(blob('c', 6)) // header+entry = 10 bytes needed 14 available but not in contiguous space
	assert.Equal(t, 50, q.Capacity())
}

func TestUnchangedEntriesAfterAdditionalSpacer(t *testing.T) {
	t.Parallel()
	q := NewByteStorageQueue(25, 0, logger)
	q.Enqueue(blob('a', 3))
	index, _ := q.Enqueue(blob('b', 6))
	q.Dequeue()
	q.Enqueue(blob('c', 6))

	assert.Equal(t, 50, q.Capacity())
	val, err := q.Get(index)
	assert.NoError(t, err)
	assert.Equal(t, blob('b', 6), val)
}

func TestAdditionalSpaceForFragmentedData(t *testing.T) {
	t.Parallel()
	q := NewByteStorageQueue(100, 0, logger)
	q.Enqueue(blob('a', 70)) // header+entry+leftMargin = 75 bytes
	q.Enqueue(blob('b', 10)) // header+entry = 14 bytes = 89 bytes
	q.Dequeue()              // space freed = 74 bytes at beginning
	q.Enqueue(blob('c', 30)) // 34 bytes used at front, now tail is before head
	q.Enqueue(blob('d', 40)) // 44 bytes needed but not available in contiguous space

	assert.Equal(t, 200, q.Capacity())
	assert.Equal(t, blob('c', 30), pop(q))
	assert.Equal(t, blob(0, 36), pop(q))
	assert.Equal(t, blob('b', 10), pop(q))
	assert.Equal(t, blob('d', 40), pop(q))

}

func TestMaxLimitReached(t *testing.T) {
	t.Parallel()
	q := NewByteStorageQueue(30, 50, logger)

	q.Enqueue(blob('a', 25))
	q.Enqueue(blob('b', 5))
	cap := q.Capacity()
	_, err := q.Enqueue(blob('c', 15))
	assert.Error(t, err)
	assert.EqualError(t, err, "queue is full, max limit of 50 reached")
	assert.Equal(t, 50, cap)
}

func blob(b byte, length int) []byte {
	buff := make([]byte, length)
	for idx := range buff {
		buff[idx] = b
	}
	return buff
}

func pop(q *ByteStorageQueue) []byte {
	val, _ := q.Dequeue()
	return val
}
