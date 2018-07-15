package gateway

import (
	"context"
	"fmt"
	"time"

	"sync"

	"github.com/coreos/etcd/clientv3"
	"github.com/pkg/errors"
)

const (
	dataSource1 = "root@tcp(127.0.0.1:13306)/cowloon"
	dataSource2 = "root@tcp(127.0.0.1:13307)/cowloon"
)

var dsMap = map[string]string{
	"1": dataSource1,
	"2": dataSource2,
}
var mu sync.RWMutex

type Router struct {
	dbKeyMap  map[string]string
	dbNameMap map[string]*Db
	etcdCli   *clientv3.Client
}

func NewRouter() (*Router, error) {
	cfg := clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: time.Second,
	}

	// TODO: Create ways to close connection.
	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}

	return &Router{
		dbKeyMap:  map[string]string{},
		dbNameMap: map[string]*Db{},
		etcdCli:   cli,
	}, nil
}

func (r *Router) GetDb(key string) (db *Db, err error) {
	dbName, ok := r.dbKeyMap[key]
	if !ok {
		dbName, err = r.fetchDbName(key)
		if err != nil {
			return
		}
		r.dbKeyMap[key] = dbName
	}

	mu.RLock()
	db, ok = r.dbNameMap[dbName]
	mu.RUnlock()
	if !ok {
		db, err = r.buildDb(dbName)
		if err != nil {
			return
		}
	}

	return
}

func (r *Router) fetchDbName(key string) (string, error) {
	resp, err := r.etcdCli.Get(context.Background(), key)
	if err != nil {
		return "", err
	}

	for _, kv := range resp.Kvs {
		return string(kv.Value), nil
	}

	return "", errors.New(fmt.Sprintf("key is not registered(key: %s)", key))
}

func (r *Router) buildDb(dbName string) (*Db, error) {
	ds, ok := dsMap[dbName]
	if !ok {
		return nil, fmt.Errorf("invalid database name: %s", dbName)
	}

	// TODO: Create ways to close connection.
	db, err := NewDb(ds)
	if err != nil {
		return nil, err
	}

	mu.Lock()
	defer mu.Unlock()
	if odb, ok := r.dbNameMap[dbName]; ok {
		err = db.Close()
		if err != nil {
			return nil, errors.Wrap(err, "cannot close database while closing duplicated connection")
		}
		db = odb
	} else {
		r.dbNameMap[dbName] = db
	}

	return db, nil
}
