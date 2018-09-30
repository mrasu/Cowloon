package migrator

import (
	"fmt"
	"strconv"

	"github.com/siddontang/go-mysql/mysql"

	"github.com/mrasu/Cowloon/pkg/db"
	"github.com/pkg/errors"
)

type MasterStatus struct {
	File     string
	Position uint32
}

func GetMasterStatus(c *db.ShardConnection) (*MasterStatus, error) {
	rows, err := c.Query("SHOW MASTER STATUS")
	if err != nil {
		return nil, errors.Wrap(err, "Invalid query: SHOW MASTER STATUS")
	}

	if len(rows) != 1 {
		return nil, fmt.Errorf("SHOW MASTER STATUS returns %d rows", len(rows))
	}

	res := rows[0]

	file := ""
	pos := 0

	for i, c := range res.Columns {
		if i == 0 {
			file = c.Value.GetValue()
		} else if i == 1 {
			v, err := strconv.Atoi(c.Value.GetValue())
			if err != nil {
				return nil, fmt.Errorf("SHOW MASTER STATUS returns %v as position)", c.Value.GetValue())
			}
			pos = v
		}
	}

	if file == "" || pos == 0 {
		return nil, fmt.Errorf("SHOW MASTER STATUS returns invalid value (file: %v, position: %v)", file, pos)
	}

	return &MasterStatus{
		File:     file,
		Position: uint32(pos),
	}, nil
}

func (ms *MasterStatus) ToMysqlPosition() mysql.Position {
	return mysql.Position{
		Name: ms.File,
		Pos:  ms.Position,
	}
}
