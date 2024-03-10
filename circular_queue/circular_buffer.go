package circular_queue

import (
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"time"
)

/*
1 ensures that there is always at least one position left unoccupied between the head and the tail when the circular_queue wraps around.
This helps distinguish between an empty circular_queue (head == tail) and a full circular_queue (head == tail + 1).
*/
const leftMarginIdx = 1 //position reserved before the head pointer

type ByteStorageQueue struct {
	storage         []byte
	capacity        int
	head            int
	tail            int
	count           int
	rightMargin     int
	logger          *log.Logger
	headerEntrySize int
	headerBuff      []byte
	mutex           sync.RWMutex
}

func NewByteStorageQueue(capacity int, logger *log.Logger) *ByteStorageQueue {
	return &ByteStorageQueue{
		storage:         make([]byte, capacity),
		capacity:        capacity,
		headerEntrySize: 4, // default value
		headerBuff:      make([]byte, 4),
		head:            leftMarginIdx,
		tail:            leftMarginIdx,
		logger:          logger,
		mutex:           sync.RWMutex{},
	}
}

func (q *ByteStorageQueue) Enqueue(data []byte) int {
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
	if q.availableSpaceAfterTail() < dataLen+q.headerEntrySize {
		if q.availableSpaceBeforeHead() >= dataLen+q.headerEntrySize {
			q.tail = leftMarginIdx
		} else {
			q.allocateAdditionalSpace()
		}
	}
	idx := q.tail
	q.push(data, dataLen)
	return idx
}

func (q *ByteStorageQueue) push(data []byte, dataLen int) {
	binary.LittleEndian.PutUint32(q.headerBuff, uint32(dataLen))
	q.copy(q.headerBuff, q.headerEntrySize)
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
	q.head += size + q.headerEntrySize
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
	dataLen := int(binary.LittleEndian.Uint32(q.storage[idx : idx+q.headerEntrySize]))
	// idx+4 to idx+4+dataLen is the data
	return q.storage[idx+q.headerEntrySize : idx+q.headerEntrySize+dataLen], dataLen
}

func (q *ByteStorageQueue) allocateAdditionalSpace() {
	start := time.Now()
	q.capacity = q.capacity * 2
	newStorage := make([]byte, q.capacity)
	copy(newStorage[leftMarginIdx:], q.storage[q.head:q.rightMargin]) //This effectively copies the contiguous portion of the used data into the new circular_queue.

	newTail := q.rightMargin - q.head + leftMarginIdx
	if q.tail <= q.head {
		copy(newStorage[newTail:], q.storage[leftMarginIdx:q.tail])
		newTail += q.tail - leftMarginIdx
	}
	q.storage = newStorage
	q.head = leftMarginIdx
	q.tail = newTail
	q.rightMargin = newTail
	q.logger.Printf("Allocated additional space %d in %v", len(newStorage), time.Since(start))
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
	return q.head - q.tail
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
		return q.capacity - leftMarginIdx
	}
	return q.head - q.tail
}
