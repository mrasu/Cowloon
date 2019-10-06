package db

import (
	"database/sql"
	"strconv"
)

func ToString(bytes sql.RawBytes) string {
	return string(bytes)
}

func ToInt(bytes sql.RawBytes) (int, error) {
	return strconv.Atoi(string(bytes))
}
