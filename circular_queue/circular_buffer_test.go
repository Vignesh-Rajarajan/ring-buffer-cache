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
	q := NewByteStorageQueue(10, logger)
	data := []byte("hello")
	idx := q.Enqueue(data)
	if idx != 1 {
		t.Errorf("expected 1, got %d", idx)
	}
	data, err := q.Dequeue()
	if err != nil {
		t.Errorf("expected nil, got %s", err)
	}
	if string(data) != "hello" {
		t.Errorf("expected hello, got %s", string(data))
	}
}

func TestPeek(t *testing.T) {
	t.Parallel()
	q := NewByteStorageQueue(10, logger)
	data := []byte("hello")
	q.Enqueue(data)
	data, _ = q.Peek()
	if string(data) != "hello" {
		t.Errorf("expected hello, got %s", string(data))
	}
}

func TestAvailableSpace(t *testing.T) {
	t.Parallel()
	q := NewByteStorageQueue(20, logger)
	q.Enqueue([]byte("hello"))
	q.Enqueue([]byte("hello1"))
	_, _ = q.Dequeue()
	q.Enqueue([]byte("hello3"))

	assert.Equal(t, 20, q.Capacity())
}

func TestAllocateSpace(t *testing.T) {
	t.Parallel()
	q := NewByteStorageQueue(10, logger)
	q.Enqueue([]byte("hello"))
	q.Enqueue([]byte("hello1"))

	assert.Equal(t, 20, q.Capacity())
}
