package reference

import (
	"fmt"
	"strings"

	"github.com/mrasu/Cowloon/pkg/migrator/tableinfo"

	"github.com/pkg/errors"

	"github.com/mrasu/Cowloon/pkg/db"
)

type References struct {
	referenceEntries map[RootKey][]*Entry
}

func NewReferences(s *db.ShardConnection, schema string, keys []RootKey) (*References, error) {
	entries := map[RootKey][]*Entry{}
	for _, k := range keys {
		entries[k] = []*Entry{}
	}
	r := &References{
		referenceEntries: entries,
	}

	err := r.validateRootKeys(s, schema)
	if err != nil {
		return nil, errors.Wrap(err, "validation to RootKeys fails")
	}

	refQ := "select REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME, TABLE_NAME, COLUMN_NAME from information_schema.KEY_COLUMN_USAGE where TABLE_SCHEMA = ? AND REFERENCED_COLUMN_NAME IS NOT NULL"
	refs, err := s.Query(refQ, schema)

	if err != nil {
		return nil, errors.Wrap(err, "failed to find references")
	}

	eMap := map[string][]*Entry{}
	for _, ref := range refs {
		cols := ref.Columns
		m := eMap[cols[0].Value.Value]
		m = append(m, NewEntry(cols[0].Value.Value, cols[1].Value.Value, cols[2].Value.Value, cols[3].Value.Value))

		eMap[cols[0].Value.Value] = m
	}

	for k, entries := range r.referenceEntries {
		res := r.appendRefs(entries, eMap, k.TableName)
		r.referenceEntries[k] = res
	}

	return r, nil
}

func (r *References) validateRootKeys(s *db.ShardConnection, schema string) error {
	var questions []string
	var tables []string
	var columns []string
	for k := range r.referenceEntries {
		tables = append(tables, k.TableName)
		columns = append(columns, k.ColumnName)
		questions = append(questions, "?")
	}
	query := fmt.Sprintf(
		"SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.columns WHERE TABLE_SCHEMA = ? AND TABLE_NAME IN (%s) AND COLUMN_NAME IN (%s)",
		strings.Join(questions, ","),
		strings.Join(questions, ","),
	)

	args := []interface{}{schema}
	for _, t := range tables {
		args = append(args, t)
	}
	for _, c := range columns {
		args = append(args, c)
	}

	rows, err := s.Query(query, args...)

	if err != nil {
		return errors.Wrap(err, "failed to find references")
	}

	roots := map[string]string{}
	for k := range r.referenceEntries {
		roots[k.TableName] = k.ColumnName
	}

	for _, r := range rows {
		tn := r.Columns[0].Value.Value
		cn := r.Columns[1].Value.Value
		if v, ok := roots[tn]; ok {
			if v == cn {
				delete(roots, tn)
			}
		}
	}

	if len(roots) != 0 {
		return fmt.Errorf("no keyColumn found while finding references")
	}

	return nil
}

func (r *References) appendRefs(entries []*Entry, eMap map[string][]*Entry, t string) []*Entry {
	if es, ok := eMap[t]; ok {
		for _, e := range es {
			entries = append(entries, e)
			entries = r.appendRefs(entries, eMap, e.referencingTable)
			fmt.Printf("%v: %v\n", t, len(entries))
		}
	}

	return entries
}

func (r *References) GetEntries(rk RootKey) []*Entry {
	return r.referenceEntries[rk]
}

func (r *References) ToMigrationTables() map[RootKey][]*tableinfo.MigrationTable {
	result := map[RootKey][]*tableinfo.MigrationTable{}
	ts := map[string]*tableinfo.MigrationTable{}
	mcs := map[string][]*tableinfo.MigratingColumn{}

	for key, entries := range r.referenceEntries {
		c := tableinfo.NewMigratingColumn(key.ColumnName)
		c.MarkMigrated(key.Key)
		t := tableinfo.NewMigrationTable(key.TableName, key.ColumnName, c)
		ts[t.Name] = t
		targets := []*tableinfo.MigrationTable{t}
		mcs[key.TableName] = []*tableinfo.MigratingColumn{c}

		for _, entry := range entries {
			var targetColumn *tableinfo.MigratingColumn

			for _, mc := range mcs[entry.referencedTable] {
				if mc.Name == entry.referencedColumn {
					targetColumn = mc
					break
				}
			}

			if targetColumn == nil {
				targetColumn = tableinfo.NewMigratingColumn(entry.referencedColumn)
				if _, exists := mcs[entry.referencedTable]; !exists {
					mcs[entry.referencedTable] = []*tableinfo.MigratingColumn{}
				}
				mcs[entry.referencedTable] = append(mcs[entry.referencedTable], targetColumn)
			}

			table := tableinfo.NewMigrationTable(entry.referencingTable, entry.referencingColumn, targetColumn)

			ts[table.Name] = table
			targets = append(targets, table)
		}

		result[key] = targets
	}

	for _, tables := range result {
		for _, table := range tables {
			if cs, ok := mcs[table.Name]; ok {
				for _, c := range cs {
					table.AddMemorizationColumn(c)
				}
			}
		}
	}

	return result
}
