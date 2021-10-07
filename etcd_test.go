package etcd_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	etcd "github.com/uccu/go-etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestAA(t *testing.T) {

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: time.Duration(2000) * time.Second,
	})

	if err != nil {
		t.Errorf(err.Error())
		return
	}

	leaseResp, _ := cli.Lease.Grant(context.TODO(), 5)
	leaseRespChan, _ := cli.Lease.KeepAlive(context.TODO(), leaseResp.ID)

	go func() {
		for leaseKeepResp := range leaseRespChan {
			if leaseKeepResp == nil {
				fmt.Println(111)
				return
			}
		}

		leaseResp, err = cli.Lease.Grant(context.TODO(), 5)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		leaseRespChan, err = cli.Lease.KeepAlive(context.TODO(), leaseResp.ID)
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		fmt.Println(222)

		for leaseKeepResp := range leaseRespChan {
			if leaseKeepResp == nil {
				fmt.Println(444)
				return
			}
		}
		fmt.Println(333)
	}()

	go func() {

		time.Sleep(2 * time.Second)

		cli.Lease.Revoke(context.TODO(), leaseResp.ID)

		time.Sleep(12 * time.Second)

		cli.Lease.Revoke(context.TODO(), leaseResp.ID)

	}()

	ti := 0
	for {
		time.Sleep(time.Second)
		ti++
	}

}

func TestAB(t *testing.T) {

	client, err := etcd.New(&etcd.Conf{
		Addr:          []string{"127.0.0.1:2379"},
		DialTimeout:   5,
		LeaseGrantTTL: 5,
	})

	if err != nil {
		t.Error(err)
		return
	}

	err = client.SetLease()
	if err != nil {
		t.Error(err)
		return
	}
	err = client.PutWithLease("service/s1", "s1")
	if err != nil {
		t.Error(err)
		return
	}
	// client.Get("service", func(key, val string) {
	// 	fmt.Printf("key:%s,val:%s", key, val)
	// })

	for {
		time.Sleep(time.Second)
		client.Get("service", func(key, val string) {
			fmt.Printf("key:%s,val:%s\n", key, val)
		})
	}
}
