package encoding

import "encoding/binary"

const (
	timestampSizeInBytes = 8
	hashSizeInBytes      = 8
	keySizeInBytes       = 2
	headerSizeInBytes    = timestampSizeInBytes + hashSizeInBytes + keySizeInBytes
)

func WrapEntry(timestamp int64, hash uint64, key string, value []byte) []byte {
	var blob []byte
	keyLen := len(key)
	blob = make([]byte, headerSizeInBytes+keyLen+len(value))
	binary.BigEndian.PutUint64(blob, uint64(timestamp))
	binary.BigEndian.PutUint64(blob[timestampSizeInBytes:], hash)
	binary.BigEndian.PutUint16(blob[hashSizeInBytes+timestampSizeInBytes:], uint16(keyLen))
	copy(blob[headerSizeInBytes:], key)
	copy(blob[headerSizeInBytes+keyLen:], value)
	return blob
}

func ReadEntry(data []byte) []byte {
	keyLen := binary.BigEndian.Uint16(data[hashSizeInBytes+timestampSizeInBytes:])
	return data[headerSizeInBytes+keyLen:]

}

func ReadKey(data []byte) string {
	keyLen := binary.BigEndian.Uint16(data[hashSizeInBytes+timestampSizeInBytes:])
	return string(data[headerSizeInBytes : headerSizeInBytes+keyLen])
}

func ReadTimestamp(data []byte) int64 {
	return int64(binary.BigEndian.Uint64(data))
}

func ReadHash(data []byte) uint64 {
	return binary.BigEndian.Uint64(data[timestampSizeInBytes:])
}

func ResetKeyFromEntry(data []byte) {
	binary.BigEndian.PutUint64(data[timestampSizeInBytes:], 0)

}
