package tableinfo

type MigratingColumn struct {
	Name string

	migratedValues    []string
	migratedValuesMap map[string]interface{}
}

func NewMigratingColumn(name string) *MigratingColumn {
	return &MigratingColumn{
		Name:              name,
		migratedValues:    []string{},
		migratedValuesMap: map[string]interface{}{},
	}
}

func (c *MigratingColumn) MigratedValues() []string {
	return c.migratedValues
}

func (c *MigratingColumn) MarkMigrated(value string) {
	if c.IsMigrationTarget(value) {
		return
	}

	c.migratedValuesMap[value] = nil
	c.migratedValues = append(c.migratedValues, value)
}

func (c *MigratingColumn) IsMigrationTarget(value string) bool {
	_, ok := c.migratedValuesMap[value]
	return ok
}
