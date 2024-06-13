package bitcask

// we scan all files and build a snapshot
// this is a very expensive operation, so we do it only once
func (db *DB) buildSnapshot() []byte {
	return nil
}

func (db *DB) installSnapshot(snapshot []byte) error {
	return nil
}
