package bitcask

import "os"

// Merge merges all the old files in the database.
func (b *DB) Merge() error {
	target := b.fileIds[len(b.fileIds)-1]
	for b.fileIds[0] < target {
		id := uint32(b.fileIds[0])
		if id == b.activeFile.FileId {
			continue
		}
		b.mu.Lock()
		f, ok := b.olderFiles[id]
		if !ok {
			continue
		}
		offset := int64(0)
		for offset < f.WriteOff {
			log, l, err := f.ReadLogRecord(offset)
			if err != nil {
				return err
			}
			if p := b.index.Get(log.Key); p != nil && p.Fid == id && p.Offset == offset {
				b.mu.Lock()
				b.index.Delete(log.Key)
				index, err := b.appendLogRecord(log)
				if err != nil {
					b.mu.Unlock()
					return err
				}
				b.index.Put(log.Key, index)
				b.mu.Unlock()
			} else {
				b.mu.Lock()
				index, err := b.appendLogRecord(log)
				if err != nil {
					b.mu.Unlock()
					return err
				}
				b.index.Put(log.Key, index)
				b.mu.Unlock()
			}
			offset += l
		}

		b.bytesWrite -= uint(f.WriteOff)
		b.reclaimSize -= f.WriteOff
		delete(b.olderFiles, id)
		err := os.Remove(f.GetFileName())
		b.mu.Unlock()

		if err != nil {
			return err
		}
		b.fileIds = b.fileIds[1:]
	}
	return nil
}
