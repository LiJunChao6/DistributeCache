package distributecache

import (
	consistenthash "DistributeCache/consistentHash"
	"context"
	"fmt"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func RegisterServer(etcdClient *clientv3.Client, server Server, ttl int64) {
	fmt.Println("Registering server:", server.ID)
	// 创建租约
	leaseGrantResp, err := etcdClient.Grant(context.Background(), ttl)
	if err != nil {
		log.Fatalf("failed to grant lease: %v", err)
	}
	leaseID := leaseGrantResp.ID

	// 注册服务器信息
	_, err = etcdClient.Put(context.Background(), "/servers/"+server.ID, server.Addr, clientv3.WithLease(leaseID))
	if err != nil {
		log.Fatalf("failed to put server info: %v", err)
	}

	// 保持租约活跃
	keepAliveChan, err := etcdClient.KeepAlive(context.Background(), leaseID)
	if err != nil {
		log.Fatalf("failed to keep alive lease: %v", err)
	}

	for {
		select {
		case <-keepAliveChan:
			// fmt.Println("lease renewal successful")
			// 租约续期成功，这里可以做一些其他事情，比如检查服务器状态
		case <-time.After(time.Duration(ttl) * time.Second):
			// 如果租约续期失败，重新尝试
			log.Println("lease renewal failed, attempting reconnect...")
			RegisterServer(etcdClient, server, ttl)
			return
		}
	}
}

func WatchServers(etcdClient *clientv3.Client, peers *consistenthash.Map) {
	for {
		rch := etcdClient.Watch(context.Background(), "/servers/", clientv3.WithPrefix())
		for wresp := range rch {
			for _, ev := range wresp.Events {
				switch ev.Type {
				case clientv3.EventTypePut:
					log.Println("Server added:", string(ev.Kv.Value))
					peers.Add(string(ev.Kv.Value))
				case clientv3.EventTypeDelete:
					log.Println("Server deleted:", string(ev.Kv.Value))
					peers.Remove(string(ev.Kv.Value))
				}
			}
		}
	}
}
