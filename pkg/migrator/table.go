package migrator

import (
	"fmt"
	"strings"

	"github.com/mrasu/Cowloon/pkg/migrator/tableinfo"

	"github.com/mrasu/Cowloon/pkg/db"
	"github.com/pkg/errors"
)

const (
	defaultCopyRange = 1000
)

type Table struct {
	migrationTable *tableinfo.MigrationTable

	appliedAtId    int
	maxMigrationId int
	Migrated       bool

	fromShard *db.ShardConnection
	toShard   *db.ShardConnection
	copyRange int
}

func NewTable(mt *tableinfo.MigrationTable, fs, ts *db.ShardConnection) (*Table, error) {
	t := &Table{
		migrationTable: mt,
		appliedAtId:    0,
		Migrated:       false,
		copyRange:      defaultCopyRange,
		fromShard:      fs,
		toShard:        ts,
	}

	id, err := t.getMaxMigrationId()
	if err != nil {
		return nil, err
	}

	t.maxMigrationId = id
	return t, nil
}

func (t *Table) name() string {
	return t.migrationTable.Name
}

func (t *Table) keyColumnName() string {
	return t.migrationTable.MigrationTargetColumnName
}

func (t *Table) getMaxMigrationId() (int, error) {
	s := fmt.Sprintf("SELECT MAX(id) FROM %s", t.name())
	columns, rows, err := t.fromShard.QueryQuery(db.NewQuery(s, []interface{}{}))
	if err != nil {
		return -1, err
	}

	if len(rows) != 1 {
		return -1, fmt.Errorf("getMaxMigrationId returns %d rows", len(rows))
	}

	row := rows[0]
	if len(columns) != 1 {
		return -1, fmt.Errorf("getMaxMigrationId returns %d columns", len(columns))
	}

	column := row[0]
	maxId, err := db.ToInt(column)
	if err != nil {
		return -1, errors.Wrap(err, fmt.Sprintf("Cannot convert(%v) to int", column))
	}

	return maxId, nil
}

func (t *Table) CopyRows() error {
	query := t.buildSelectCopyRangeQuery()
	columnNames, rows, err := t.fromShard.QueryQuery(query)

	if err != nil {
		return err
	}

	if len(rows) == 0 {
		t.Migrated = true
		return nil
	}

	var values []string
	var args []interface{}
	var lastId int
	for _, row := range rows {
		var vs []string
		for i, column := range row {
			vs = append(vs, "?")
			args = append(args, column)

			if columnNames[i] == "id" {
				lastId, err = db.ToInt(column)
				if err != nil {
					return err
				}
			}

			for _, mc := range t.migrationTable.MemorizationColumns {
				if columnNames[i] == mc.Name {
					mc.MarkMigrated(db.ToString(column))
				}
			}
		}

		values = append(values, "("+strings.Join(vs, ", ")+")")
	}
	insertQuery := t.buildReplaceQueryString(columnNames, values)

	_, err = t.toShard.Exec(insertQuery, args...)
	if err != nil {
		return err
	}

	t.appliedAtId = lastId
	if len(rows) < t.copyRange {
		t.Migrated = true
	}

	return nil
}

func (t *Table) buildSelectCopyRangeQuery() *db.Query {
	vals := t.migrationTable.MigrationTargetColumn.MigratedValues()
	var inCondition []string
	for range vals {
		inCondition = append(inCondition, "?")
	}

	sqlText := fmt.Sprintf(
		"SELECT * FROM %s WHERE id > ? AND id <= ? AND %s IN (%s) ORDER BY id LIMIT ?",
		t.name(),
		t.keyColumnName(),
		strings.Join(inCondition, ","),
	)

	args := []interface{}{t.appliedAtId, t.maxMigrationId}
	for _, v := range vals {
		args = append(args, v)
	}
	args = append(args, t.copyRange)

	return db.NewQuery(
		sqlText,
		args,
	)
}

func (t *Table) buildReplaceQueryString(columnNames, valuesList []string) string {
	var duplicates []string
	for _, c := range columnNames {
		duplicates = append(duplicates, c+"=values("+c+")")
	}

	// Not user "REPLACE INTO" to avoid failure by foreign key constraints
	return fmt.Sprintf(
		"INSERT INTO %s(%s) VALUES%s ON DUPLICATE KEY UPDATE %s",
		t.name(),
		strings.Join(columnNames, ","),
		strings.Join(valuesList, ", "),
		strings.Join(duplicates, ", "),
	)
}
