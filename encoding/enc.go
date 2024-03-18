package encoding

import "encoding/binary"

const (
	timestampSizeInBytes = 8
	hashSizeInBytes      = 8
	keySizeInBytes       = 2
	// HeaderSizeInBytes is the size of the header in bytes
	HeaderSizeInBytes = timestampSizeInBytes + hashSizeInBytes + keySizeInBytes
)

func WrapEntry(timestamp int64, hash uint64, key string, value []byte, buffer *[]byte) []byte {
	var blob []byte
	keyLen := len(key)
	blobLen := len(value) + keyLen + HeaderSizeInBytes
	if blobLen > len(*buffer) {
		*buffer = make([]byte, blobLen)
	}
	blob = *buffer
	blob = make([]byte, HeaderSizeInBytes+keyLen+len(value))
	binary.BigEndian.PutUint64(blob, uint64(timestamp))
	binary.BigEndian.PutUint64(blob[timestampSizeInBytes:], hash)
	binary.BigEndian.PutUint16(blob[hashSizeInBytes+timestampSizeInBytes:], uint16(keyLen))
	copy(blob[HeaderSizeInBytes:], key)
	copy(blob[HeaderSizeInBytes+keyLen:], value)
	return blob[:blobLen]
}

func ReadEntry(data []byte) []byte {
	keyLen := binary.BigEndian.Uint16(data[hashSizeInBytes+timestampSizeInBytes:])
	dst := make([]byte, len(data)-int(HeaderSizeInBytes+keyLen))
	copy(dst, data[HeaderSizeInBytes+keyLen:])
	return dst

}

func ReadKey(data []byte) string {
	keyLen := binary.BigEndian.Uint16(data[hashSizeInBytes+timestampSizeInBytes:])
	dst := make([]byte, keyLen)
	copy(dst, data[HeaderSizeInBytes:HeaderSizeInBytes+keyLen])
	return string(dst)
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
