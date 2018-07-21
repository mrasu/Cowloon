package gateway

import (
	"context"
	"fmt"
	"time"

	"sync"

	"github.com/coreos/etcd/clientv3"
	"github.com/mrasu/Cowloon/pkg/db"
	"github.com/pkg/errors"
)

const (
	shardKeyPrefix = "shardKey/"
)

var dataSource1 = []string{"root", "127.0.0.1:13306", "cowloon"}
var dataSource2 = []string{"root", "127.0.0.1:13307", "cowloon"}

var dsMap = map[string][]string{
	"1": dataSource1,
	"2": dataSource2,
}
var mu sync.RWMutex

type Router struct {
	shardKeyMap map[string]string
	shardMap    map[string]*db.ShardConnection
	etcdCli     *clientv3.Client
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
		shardKeyMap: map[string]string{},
		shardMap:    map[string]*db.ShardConnection{},
		etcdCli:     cli,
	}, nil
}

func (r *Router) GetShardConnection(key string) (s *db.ShardConnection, err error) {
	shardName, ok := r.shardKeyMap[key]
	if !ok {
		shardName, err = r.fetchDbName(key)
		if err != nil {
			return
		}
		r.shardKeyMap[key] = shardName
	}

	mu.RLock()
	s, ok = r.shardMap[shardName]
	mu.RUnlock()
	if !ok {
		s, err = r.buildDb(shardName)
		if err != nil {
			return
		}
	}

	return
}

func (r *Router) fetchDbName(key string) (string, error) {
	ek := r.toEtcdShardKey(key)
	resp, err := r.etcdCli.Get(context.Background(), ek)
	if err != nil {
		return "", err
	}

	for _, kv := range resp.Kvs {
		return string(kv.Value), nil
	}

	return "", errors.New(fmt.Sprintf("key is not registered(key: %s)", key))
}

func (r *Router) buildDb(dbName string) (*db.ShardConnection, error) {
	ds, ok := dsMap[dbName]
	if !ok {
		return nil, fmt.Errorf("invalid database name: %s", dbName)
	}

	// TODO: Create ways to close connection.
	sc, err := db.NewShardConnection(ds[0], ds[1], ds[2])
	if err != nil {
		return nil, err
	}

	mu.Lock()
	defer mu.Unlock()
	if odb, ok := r.shardMap[dbName]; ok {
		err = sc.Close()
		if err != nil {
			return nil, errors.Wrap(err, "cannot close database while closing duplicated connection")
		}
		sc = odb
	} else {
		r.shardMap[dbName] = sc
	}

	return sc, nil
}

func (r *Router) RegisterKey(key, shardName string) error {
	if _, ok := dsMap[shardName]; !ok {
		return fmt.Errorf("shard not found: %s", shardName)
	}

	if len(key) == 0 {
		return errors.New("key is empty")
	}

	ek := r.toEtcdShardKey(key)

	_, err := r.etcdCli.Put(context.Background(), ek, shardName)
	if err != nil {
		return err
	}
	fmt.Printf("Register: %s: %s\n", ek, shardName)
	return nil
}

func (r *Router) RemoveKey(key string) error {
	if len(key) == 0 {
		return errors.New("key is empty")
	}

	ek := r.toEtcdShardKey(key)
	resp, err := r.etcdCli.Delete(context.Background(), ek)
	if err != nil {
		return err
	}

	if resp.Deleted == 0 {
		return fmt.Errorf("not key found: %s", key)
	}

	delete(r.shardKeyMap, key)
	fmt.Printf("Remove: %s: %s\n", ek, key)

	return nil
}

func (r *Router) toEtcdShardKey(rawKey string) string {
	return shardKeyPrefix + rawKey
}
