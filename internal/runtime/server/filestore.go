package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"com.lwc.raft/internal/core"
)

type fileAppendEntry struct {
	Term         uint32 `json:"term"`
	CounterDelta uint16 `json:"counter_delta"`
	Action       string `json:"action"`
}

type FileStore struct {
	Cond *sync.Cond

	processing bool
	Path       string
}

func (fs *FileStore) Append(entry core.AppendEntry) {
	fs.ReplaceFrom(fs.Restore().Len()+1, core.AppendEntries{entry})
}

func (fs *FileStore) ReplaceFrom(idx uint32, entries core.AppendEntries) {
	fs.Cond.L.Lock()
	defer fs.Cond.L.Unlock()
	fs.processing = true
	defer func() {
		fs.processing = false
		fs.Cond.Broadcast()
	}()

	fileIdx := idx - 1

	existing := fs.Restore()

	if existing == nil {
		existing = core.AppendEntries{}
	}

	if fileIdx > uint32(len(existing)) {
		return
	}

	updated := append(existing[:fileIdx], entries...)
	encoded, err := encodeReadableAppendEntries(updated)
	if err != nil {
		return
	}

	dir := filepath.Dir(fs.Path)
	tmp, err := os.CreateTemp(dir, "filestore-*.tmp")
	if err != nil {
		return
	}
	tmpPath := tmp.Name()

	if _, err := tmp.Write(encoded); err != nil {
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

func (fs *FileStore) WaitForDone() {
	fs.Cond.L.Lock()
	defer fs.Cond.L.Unlock()

	for fs.processing {
		fs.Cond.Wait()
	}
}

func (fs *FileStore) Restore() core.AppendEntries {
	bs, err := os.ReadFile(fs.Path)
	if err != nil {
		return nil
	}

	if len(bs) == 0 {
		return nil
	}

	entries, err := decodeReadableAppendEntries(bs)
	if err != nil {
		return nil
	}

	return entries
}

func encodeReadableAppendEntries(entries core.AppendEntries) ([]byte, error) {
	if len(entries) == 0 {
		return nil, nil
	}

	readable := make([]fileAppendEntry, len(entries))
	for i, entry := range entries {
		action, err := encodeReadableAction(entry.Action)
		if err != nil {
			return nil, err
		}

		readable[i] = fileAppendEntry{
			Term:         entry.Term,
			CounterDelta: entry.CounterDelta,
			Action:       action,
		}
	}

	bs, err := json.MarshalIndent(readable, "", "  ")
	if err != nil {
		return nil, err
	}

	return append(bs, '\n'), nil
}

func decodeReadableAppendEntries(bs []byte) (core.AppendEntries, error) {
	var readable []fileAppendEntry
	if err := json.Unmarshal(bs, &readable); err != nil {
		return nil, err
	}
	if len(readable) == 0 {
		return nil, nil
	}

	entries := make(core.AppendEntries, len(readable))
	for i, entry := range readable {
		action, err := decodeReadableAction(entry.Action)
		if err != nil {
			return nil, err
		}

		entries[i] = core.AppendEntry{
			Term:         entry.Term,
			CounterDelta: entry.CounterDelta,
			Action:       action,
		}
	}

	return entries, nil
}

func encodeReadableAction(action core.StateAction) (string, error) {
	switch action {
	case core.STATE_ADD:
		return "add", nil
	case core.STATE_MINUS:
		return "minus", nil
	case core.STATE_FLIP:
		return "flip", nil
	default:
		return "", core.STATE_INVAILD
	}
}

func decodeReadableAction(action string) (core.StateAction, error) {
	switch action {
	case "add":
		return core.STATE_ADD, nil
	case "minus":
		return core.STATE_MINUS, nil
	case "flip":
		return core.STATE_FLIP, nil
	default:
		return 0, core.STATE_INVAILD
	}
}
