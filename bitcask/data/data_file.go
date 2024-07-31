package data

import (
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"raft-kv/bitcask/io"
)

const (
	FileNameSuffix        = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
	SeqNoFileName         = "seq-no"
)

var (
	ErrInvalidCRC = errors.New("invalid crc")
)

type File struct {
	FileId    uint32
	WriteOff  int64
	IoManager io.Manager
}

func CreateDataFile(dirPath string, fileId uint32) error {
	fileName := GetDataFileName(dirPath, fileId)
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	return file.Sync()
}

func OpenDataFile(dirPath string, fileId uint32, ioType io.FileIOType) (*File, error) {
	fileName := GetDataFileName(dirPath, fileId)
	return newDataFile(fileName, fileId, ioType)
}

func OpenHintFile(dirPath string) (*File, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0, io.FIO)
}

func OpenMergeFinishedFile(dirPath string) (*File, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0, io.FIO)
}

func OpenSeqNoFIle(dirPath string) (*File, error) {
	fileName := filepath.Join(dirPath, SeqNoFileName)
	return newDataFile(fileName, 0, io.FIO)
}

func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+FileNameSuffix)
}

func newDataFile(fileName string, fileId uint32, ioType io.FileIOType) (*File, error) {
	ioManager, err := io.NewManager(fileName, ioType)
	if err != nil {
		return nil, err
	}
	fileSize, err := ioManager.Size()
	return &File{
		FileId:    fileId,
		WriteOff:  fileSize,
		IoManager: ioManager,
	}, nil
}

func (df *File) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IoManager.Size()
	if offset >= fileSize {
		return nil, 0, io.ErrEOF
	}
	if err != nil {
		return nil, 0, err
	}

	// get header first
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+headerBytes > fileSize {
		headerBytes = fileSize - offset
	}
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}
	// decode header
	header, headerSize := decodeLogRecordHeader(headerBuf)
	if header == nil {
		return nil, 0, io.ErrEOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.ErrEOF
	}

	// get key and value size
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize

	logRecord := &LogRecord{
		Type: header.recordType,
	}

	// read key and value
	if keySize > 0 || valueSize > 0 {
		keyBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		logRecord.Key = keyBuf[:keySize]
		logRecord.Value = keyBuf[keySize:]
	}

	// crc check
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	return logRecord, recordSize, nil
}

func (df *File) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

func (df *File) WriteHintRecord(key []byte, pos *LogRecordIndex) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordIndex(pos),
	}
	encRecord, _ := EncodeLogRecord(record)
	return df.Write(encRecord)
}

func (df *File) Sync() error {
	return df.IoManager.Sync()
}

func (df *File) Close() error {
	return df.IoManager.Close()
}

func (df *File) SetIOManager(dirPath string, ioType io.FileIOType) error {
	if err := df.IoManager.Close(); err != nil {
		return err
	}
	ioManager, err := io.NewManager(GetDataFileName(dirPath, df.FileId), ioType)
	if err != nil {
		return err
	}
	df.IoManager = ioManager
	return nil
}

func (df *File) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IoManager.Read(b, offset)
	return
}

func (df *File) GetFileName() string {
	return df.IoManager.GetFileName()
}
