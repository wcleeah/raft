package server

import (
	"os"
	"path/filepath"

	"com.lwc.raft/internal/core"
)

type FileStore struct {
	Path string
}

func (fs *FileStore) ReplaceFrom(idx uint32, entries core.AppendEntries) {
	fileIdx := idx - 1

	existing := fs.Restore()

	if existing == nil {
		existing = core.AppendEntries{}
	}

	if fileIdx > uint32(len(existing)) {
		return
	}

	updated := append(existing[:fileIdx], entries...)

	dir := filepath.Dir(fs.Path)
	tmp, err := os.CreateTemp(dir, "filestore-*.tmp")
	if err != nil {
		return
	}
	tmpPath := tmp.Name()

	if _, err := tmp.Write(updated.Encode()); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return
	}

	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return
	}

	os.Rename(tmpPath, fs.Path)
}

func (fs *FileStore) Restore() core.AppendEntries {
	bs, err := os.ReadFile(fs.Path)
	if err != nil {
		return nil
	}

	if len(bs) == 0 {
		return nil
	}

	return core.DecodeAppendEntries(bs)
}
