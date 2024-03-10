package circular_queue

import (
	"encoding/binary"
	"fmt"
	"log"
	"sync"
)

/*
1 ensures that there is always at least one position left unoccupied between the head and the tail when the circular_queue wraps around.
This helps distinguish between an empty circular_queue (head == tail) and a full circular_queue (head == tail + 1).
*/
const (
	leftMarginIdx   = 1
	headerEntrySize = 4
	minimumCapacity = 32 + headerEntrySize
) //position reserved before the head pointer

type ByteStorageQueue struct {
	storage     []byte
	capacity    int
	head        int
	tail        int
	count       int
	rightMargin int
	logger      *log.Logger
	headerBuff  []byte
	mutex       sync.RWMutex
	maxCapacity int
}

func NewByteStorageQueue(capacity, maxCapacity int, logger *log.Logger) *ByteStorageQueue {
	return &ByteStorageQueue{
		storage:     make([]byte, capacity),
		capacity:    capacity,
		headerBuff:  make([]byte, 4),
		head:        leftMarginIdx,
		tail:        leftMarginIdx,
		logger:      logger,
		rightMargin: leftMarginIdx,
		mutex:       sync.RWMutex{},
		maxCapacity: maxCapacity,
	}
}

func (q *ByteStorageQueue) Enqueue(data []byte) (int, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	dataLen := len(data)

	/**
	Let's say the circular_queue capacity is 10, dataLen is 4,
	q.headerEntrySize is 1, leftMarginIdx is 1, and the current state is: Head: 8, Tail: 9
	In this case, q.availableSpaceAfterTail() is 1 (10 - 9),
	which is less than dataLen+q.headerEntrySize (4 + 1 = 5).
	However, q.availableSpaceBeforeHead() is 7 (10 - 1), which is greater than or equal to 5.
	So, the code will set q.tail to 1, wrapping around the tail to the beginning of the circular_queue after the left margin.
	*/
	if q.availableSpaceAfterTail() < dataLen+headerEntrySize {
		if q.availableSpaceBeforeHead() >= dataLen+headerEntrySize {
			q.tail = leftMarginIdx
		} else if q.capacity+headerEntrySize+dataLen >= q.maxCapacity && q.maxCapacity > 0 {
			return -1, fmt.Errorf("queue is full, max limit of %d reached", q.maxCapacity)
		} else {
			q.allocateAdditionalSpace(dataLen + headerEntrySize)
		}
	}
	idx := q.tail
	q.push(data, dataLen)
	return idx, nil
}

func (q *ByteStorageQueue) push(data []byte, dataLen int) {
	binary.LittleEndian.PutUint32(q.headerBuff, uint32(dataLen))
	q.copy(q.headerBuff, headerEntrySize)
	q.copy(data, dataLen)
	if q.tail > q.head {
		q.rightMargin = q.tail
	}
	q.count++
}

func (q *ByteStorageQueue) copy(data []byte, dataLen int) {
	q.tail += copy(q.storage[q.tail:], data[:dataLen])
}

func (q *ByteStorageQueue) Dequeue() ([]byte, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.count == 0 {
		return nil, fmt.Errorf("queue is empty")
	}
	data, size := q.peek(q.head)
	q.head += size + headerEntrySize
	q.count--
	if q.head == q.rightMargin {
		q.head = leftMarginIdx
		if q.tail == q.rightMargin {
			q.tail = leftMarginIdx
		}
		q.rightMargin = q.tail
	}
	return data, nil

}

func (q *ByteStorageQueue) Peek() ([]byte, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.count == 0 {
		return nil, fmt.Errorf("queue is empty")
	}
	data, _ := q.peek(q.head)
	return data, nil
}

func (q *ByteStorageQueue) Get(idx int) ([]byte, error) {
	q.mutex.RLock()
	defer q.mutex.RUnlock()
	if idx <= 0 {
		return nil, fmt.Errorf("invalid index")
	}
	data, _ := q.peek(idx)
	return data, nil
}

func (q *ByteStorageQueue) Size() int {
	return q.count
}

func (q *ByteStorageQueue) Capacity() int {
	return q.capacity
}

func (q *ByteStorageQueue) peek(idx int) ([]byte, int) {
	// idx to idx+4 is the header
	dataLen := int(binary.LittleEndian.Uint32(q.storage[idx : idx+headerEntrySize]))
	// idx+4 to idx+4+dataLen is the data
	return q.storage[idx+headerEntrySize : idx+headerEntrySize+dataLen], dataLen
}

func (q *ByteStorageQueue) allocateAdditionalSpace(minLen int) {
	//start := time.Now()
	if q.capacity < minLen {
		q.capacity += minLen
	}
	q.capacity = q.capacity * 2
	if q.capacity > q.maxCapacity && q.maxCapacity > 0 {
		q.capacity = q.maxCapacity
	}
	oldBuff := q.storage
	q.storage = make([]byte, q.capacity)

	if leftMarginIdx != q.rightMargin {
		copy(q.storage, oldBuff[:q.rightMargin])
		if q.tail < q.head {
			buffLen := q.head - q.tail - headerEntrySize
			q.push(make([]byte, buffLen), buffLen)
			q.head = leftMarginIdx
			q.tail = q.rightMargin
		}
	}

	//q.logger.Printf("Allocated additional space %d in %v", len(q.storage), time.Since(start))
}

// This method calculates the available space in the circular_queue after the current tail position
// If q.tail = 3, q.head = 5, and q.capacity = 10, the function will return 5 - 3 = 2, because there are 2 free spaces after the tail index.
// If q.tail = 7, q.head = 2, and q.capacity = 10, the function will return 10 - 7 = 3, because there are 3 free spaces from the tail index to the end of the queue.
func (q *ByteStorageQueue) availableSpaceAfterTail() int {
	//If the tail position is greater than the head position, it means the used portion is contiguous
	if q.tail >= q.head {
		return q.capacity - q.tail
	}
	//If the tail position is less than or equal to the head position
	//it means the used portion of the circular_queue wraps around the end of the circular_queue
	return q.head - q.tail - minimumCapacity
}

// tells you how much space is available to write before the current head position, either contiguously or wrapping around,
// while maintaining a certain leftMarginIdx.
func (q *ByteStorageQueue) availableSpaceBeforeHead() int {
	/**
	Head: 2, Tail: 8, q.leftMarginIdx = 1
	q.tail >= q.head is true (8 >= 2)
	Available space before head = q.capacity - q.leftMarginIdx = 10 - 1 = 9 (positions 2, 3, 4, 5, 6, 7, 8, 9, and 0)
	Case 2: Tail is less than head (Used portion is contiguous)

	Head: 6, Tail: 3
	q.tail >= q.head is false (3 < 6)
	Available space before head = q.head - q.tail = 6 - 3 = 3 (positions 3, 4, and 5)
	*/

	if q.tail >= q.head {
		return q.head - leftMarginIdx - minimumCapacity
	}
	return q.head - q.tail - minimumCapacity
}
