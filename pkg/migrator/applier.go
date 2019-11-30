package migrator

import (
	"fmt"
	"log"

	"github.com/mrasu/Cowloon/pkg/migrator/reference"

	"time"

	"github.com/mrasu/Cowloon/pkg/db"
	"github.com/siddontang/go-mysql/canal"
)

type Applier struct {
	keyValue string

	fromShard *db.ShardConnection
	toShard   *db.ShardConnection

	// TODO: Change "id" to "unique key" to support tables without "id" column
	appliedAtId    int
	maxMigrationId int
	migrated       bool
}

func NewApplier(kv string, fromShard, toShard *db.ShardConnection) *Applier {
	return &Applier{
		keyValue:    kv,
		toShard:     toShard,
		fromShard:   fromShard,
		appliedAtId: 0,
		migrated:    false,
	}
}

func (a *Applier) Run(migratedCallback func() bool) error {
	cfg := &canal.Config{
		ServerID: 1,
		Flavor:   "mysql",
		Addr:     a.fromShard.Addr,
		User:     a.fromShard.User,
	}

	c, err := canal.NewCanal(cfg)
	if err != nil {
		panic(err)
	}

	ms, err := GetMasterStatus(a.fromShard)
	if err != nil {
		panic(err)
	}

	keys := []reference.RootKey{{TableName: "users", ColumnName: "tenant_id", Key: a.keyValue}}
	refs, err := reference.NewReferences(a.fromShard, "cowloon", keys)
	if err != nil {
		return err
	}
	fmt.Printf("%v\n", refs)
	dmlEventChan := make(chan []*db.Query)

	migrationTables := refs.ToMigrationTables()
	ki, ti := 0, 0
	firstTable := migrationTables[keys[ki]][ti]

	cHandler := NewCanalHandler(a.fromShard.DbName, dmlEventChan)
	cHandler.AddTable(firstTable)
	c.SetEventHandler(cHandler)
	go c.RunFrom(ms.ToMysqlPosition())

	currentCopingTable, err := NewTable(firstTable, a.fromShard, a.toShard)
	if err != nil {
		return err
	}

	for {
		select {
		case queries := <-dmlEventChan:
			{
				err := a.toShard.ExecStrictQueries(queries)
				if err != nil {
					return err
				}
			}
		default:
			{
				if currentCopingTable.Migrated {
					hasNext := false
					if len(migrationTables[keys[ki]]) > ti+1 {
						ti++
						hasNext = true
					} else if len(migrationTables) > ki+1 {
						ki++
						ti = 0
						hasNext = true
					}

					if hasNext {
						mt := migrationTables[keys[ki]][ti]
						t, err := NewTable(mt, a.fromShard, a.toShard)
						if err != nil {
							return err
						}
						cHandler.AddTable(mt)
						currentCopingTable = t
					} else {
						log.Println("do nothing...")
						res := migratedCallback()
						if res {
							return nil
						}
						time.Sleep(time.Second)
					}
					break
				}

				if err = currentCopingTable.CopyRows(); err != nil {
					return err
				}
				// if currentCopingTable.Migrated {
				// 	cHandler.addTable(currentCopingTable)
				// 	currentCopingTable = nextTable
				// }
			}
		}
	}
	return nil
}
