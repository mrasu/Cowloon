package tableinfo

type MigrationTable struct {
	Name                      string
	MigrationTargetColumnName string
	MigrationTargetColumn     *MigratingColumn

	MemorizationColumns []*MigratingColumn
}

func NewMigrationTable(tn, cn string, target *MigratingColumn) *MigrationTable {
	return &MigrationTable{
		Name: tn,
		MigrationTargetColumnName: cn,
		MigrationTargetColumn:     target,
		MemorizationColumns:       []*MigratingColumn{},
	}
}

func (t *MigrationTable) AddMemorizationColumn(c *MigratingColumn) {
	t.MemorizationColumns = append(t.MemorizationColumns, c)
}
