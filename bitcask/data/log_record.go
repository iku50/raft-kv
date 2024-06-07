package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// LogRecord is the data structure that we use to store our key-value pairs.
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// crc type keySize valueSize
// 4 + 1 + 5 + 5 = 15
const maxLogRecordHeaderSize = 15

// LogRecord 's header
type logRecordHeader struct {
	crc        uint32
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
}

type LogRecordIndex struct {
	Fid    uint32 // fid shows which file the record is stored in
	Offset int64  // offset shows the position of the record in the file
	Size   uint32 // size shows the size of the record on disk
}

type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordIndex
}

// Binary format:
// +-----------+------------+-------------+--------------+-----------+---------------+
// | crc       |  type   	|  key size   |  value size  |    key    |     value     |
// +-----------+------------+-------------+--------------+-----------+---------------+
// | 4 bytes   |  1 bytes	|dynamic(max5)| dynamic(max5)|  dynamic  |    dynamic    |
// +-----------+------------+-------------+--------------+-----------+---------------+

func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	header := make([]byte, maxLogRecordHeaderSize)
	header[4] = logRecord.Type
	var index = 5

	// put key size and value size
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	var size = index + len(logRecord.Key) + len(logRecord.Value)
	bs := make([]byte, size)

	copy(bs[:index], header[:index])
	copy(bs[index:], logRecord.Key)
	copy(bs[index+len(logRecord.Key):], logRecord.Value)

	crc := crc32.ChecksumIEEE(bs[4:])
	binary.LittleEndian.PutUint32(bs[:4], crc)

	return bs, int64(size)
}

func EncodeLogRecordIndex(pos *LogRecordIndex) []byte {
	buf := make([]byte, binary.MaxVarintLen32*2+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	index += binary.PutVarint(buf[index:], int64(pos.Size))
	return buf[:index]
}

func DecodeLogRecordIndex(buf []byte) *LogRecordIndex {
	var index = 0
	fileId, n := binary.Varint(buf[index:])
	index += n
	offset, n := binary.Varint(buf[index:])
	index += n
	size, _ := binary.Varint(buf[index:])
	return &LogRecordIndex{
		Fid:    uint32(fileId),
		Offset: offset,
		Size:   uint32(size),
	}
}

func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var index = 5
	keySize, n := binary.Varint(buf[index:])
	index += n
	header.keySize = uint32(keySize)
	valueSize, n := binary.Varint(buf[index:])
	index += n
	header.valueSize = uint32(valueSize)

	return header, int64(index)
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}
