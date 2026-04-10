package core

type Store interface {
	ReplaceFrom(uint32, AppendEntries) 
	Restore() AppendEntries
}

