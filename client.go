package etcd

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Client struct {
	client           *clientv3.Client
	conf             *Conf
	leaseID          clientv3.LeaseID
	watchCancelFuncs map[string]context.CancelFunc
	watchmu          *sync.Mutex
}

type Conf struct {
	Addr          []string
	DialTimeout   int
	LeaseGrantTTL int64
}

func New(conf *Conf) (*Client, error) {
	// 创建客户端
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   conf.Addr,
		DialTimeout: time.Duration(conf.DialTimeout) * time.Second,
	})

	if err != nil {
		return nil, err
	}

	return &Client{
		client:           client,
		conf:             conf,
		watchCancelFuncs: make(map[string]context.CancelFunc),
		watchmu:          new(sync.Mutex),
	}, nil
}

func (reg *Client) leaseGrant() error {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(reg.conf.DialTimeout)*time.Second)
	leaseResp, err := reg.client.Lease.Grant(ctx, reg.conf.LeaseGrantTTL)
	cancel()
	if err != nil {
		return err
	}
	reg.leaseID = leaseResp.ID
	return nil
}

func (reg *Client) leaseKeepAlive() error {
	leaseRespChan, err := reg.client.Lease.KeepAlive(context.TODO(), reg.leaseID)
	if err != nil {
		return err
	}

	go func() {
		for leaseKeepResp := range leaseRespChan {
			if leaseKeepResp == nil {
				return
			}
		}
	}()

	return nil
}

func (reg *Client) SetLease() error {

	var err error
	err = reg.leaseGrant()
	if err != nil {
		return err
	}

	err = reg.leaseKeepAlive()
	if err != nil {
		return err
	}

	return nil
}

func (reg *Client) Put(key, val string) error {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(reg.conf.DialTimeout)*time.Second)
	_, err := reg.client.KV.Put(ctx, key, val)
	cancel()
	return err
}

func (reg *Client) PutWithLease(key, val string) error {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(reg.conf.DialTimeout)*time.Second)
	_, err := reg.client.KV.Put(ctx, key, val, clientv3.WithLease(reg.leaseID))
	cancel()
	return err
}

func (reg *Client) Del(key string) error {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(reg.conf.DialTimeout)*time.Second)
	_, err := reg.client.KV.Delete(ctx, key)
	cancel()
	return err
}

func (client *Client) Close() error {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(client.conf.DialTimeout)*time.Second)
	_, err := client.client.Lease.Revoke(ctx, client.leaseID)
	cancel()
	client.CleanAllWatch()
	return err
}

var ErrWatchExist = errors.New("watch exist")

func (w *Client) Watch(prefix string, put func(string, string), del func(string)) error {

	w.watchmu.Lock()
	if _, ok := w.watchCancelFuncs[prefix]; ok {
		w.watchmu.Unlock()
		return ErrWatchExist
	}
	ctx, cancel := context.WithCancel(context.TODO())
	w.watchCancelFuncs[prefix] = cancel
	w.watchmu.Unlock()

	watchChan := w.client.Watch(ctx, prefix, clientv3.WithPrefix())

	go func() {
		for wresp := range watchChan {
			for _, ev := range wresp.Events {
				switch ev.Type {
				case mvccpb.PUT:
					put(string(ev.Kv.Key), string(ev.Kv.Value))
				case mvccpb.DELETE:
					del(string(ev.Kv.Key))
				}
			}
		}
	}()

	return nil
}

func (w *Client) UnWatch(prefix string) {
	w.watchmu.Lock()
	if cancel, ok := w.watchCancelFuncs[prefix]; ok {
		cancel()
		delete(w.watchCancelFuncs, prefix)
	}
	w.watchmu.Unlock()
}

func (w *Client) CleanAllWatch() {
	w.watchmu.Lock()
	for _, cancel := range w.watchCancelFuncs {
		cancel()
	}
	w.watchCancelFuncs = nil
	w.watchCancelFuncs = make(map[string]context.CancelFunc)

	w.watchmu.Unlock()
}

func (w *Client) Get(prefix string, get func(key, value string)) error {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(w.conf.DialTimeout)*time.Second)
	resp, err := w.client.Get(ctx, prefix, clientv3.WithPrefix())
	cancel()
	if err != nil {
		return err
	}

	for i := range resp.Kvs {
		get(string(resp.Kvs[i].Key), string(resp.Kvs[i].Value))
	}

	return nil
}
