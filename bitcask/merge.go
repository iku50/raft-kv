package bitcask

// Merge merges all the old files in the database.
func (b *DB) Merge() error {
	ml := int64(0)
	for i := 0; i < len(b.olderFiles); i-- {
		id := uint32(b.fileIds[i])
		if id == b.activeFile.FileId {
			continue
		}
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
			if p := b.index.Get(log.Key); p.Fid == id && p.Offset == offset {
				ml += l
				b.mu.Lock()
				b.index.Delete(log.Key)
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
		b.mu.Lock()
		b.bytesWrite -= uint(f.WriteOff)
		b.reclaimSize -= f.WriteOff
		delete(b.olderFiles, id)
		b.fileIds = append(b.fileIds[:i], b.fileIds[i+1:]...)
	}
	return nil
}
