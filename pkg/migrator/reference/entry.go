package reference

type Entry struct {
	referencedTable  string
	referencedColumn string

	referencingTable  string
	referencingColumn string
}

func NewEntry(refTable, refColumn, refedTable, refedColumn string) *Entry {
	return &Entry{
		referencedTable:   refTable,
		referencedColumn:  refColumn,
		referencingTable:  refedTable,
		referencingColumn: refedColumn,
	}
}
