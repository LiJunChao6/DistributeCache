package main

import (
	distributecache "DistributeCache"
	"DistributeCache/codec"
	consistenthash "DistributeCache/consistentHash"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var db = map[string]string{
	"Tom": "630",
	"ngs": "567",
}

const (
	userPath          = "/_geerpc_/users"
	baseAddr          = "0.0.0.0:9999"
	etcdEndpoints     = "http://localhost:2379"
	leaseTTL          = 60
	defaultRPCReplice = 10
)

// 注册中心服务器
func startAPIServer(wg *sync.WaitGroup) {
	peers := consistenthash.New(defaultRPCReplice, nil)

	http.HandleFunc(userPath, func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "Missing key", http.StatusBadRequest)
			return
		}
		rpcAddr := peers.Get(key)
		if rpcAddr == "" {
			err := fmt.Errorf("rpc server: %s", "rpcAddr not found")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte(rpcAddr))
	})

	dEndpoints := []string{etcdEndpoints}
	// 初始化 Etcd 客户端
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   dEndpoints,
		DialTimeout: time.Second,
	})
	if err != nil {
		log.Fatalf("failed to create etcd client: %v", err)
	}
	go distributecache.WatchServers(etcdClient, peers)

	// 启动一个 HTTP 服务器来处理来自用户的请求
	log.Printf("API server listening on %s", baseAddr)
	if err := http.ListenAndServe(baseAddr, nil); err != nil {
		log.Fatalf("API server failed: %v", err)
	}
	wg.Done()
}

// 分布式节点服务器
func startserver(rpcAddr string, wg *sync.WaitGroup) {
	gee := distributecache.NewGroup("ljc", 2<<10, distributecache.GetterFunc(func(key string) ([]byte, error) {
		log.Println("[SlowDB] search key", key)
		if v, ok := db[key]; ok {
			return []byte(v), nil
		}
		return nil, fmt.Errorf("%s not exist", key)
	}))

	l, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		log.Fatalf("rpc server: listen error: %v", err)
	}

	ip, port, _ := net.SplitHostPort(l.Addr().String())
	// 使用雪花算法生成全局唯一ID
	serverID := distributecache.GenerateID(ip, port)
	id := strconv.FormatInt(serverID, 10)
	fmt.Println("serverID:", id)

	dEndpoints := []string{etcdEndpoints}
	// 初始化 Etcd 客户端
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   dEndpoints,
		DialTimeout: time.Second,
	})
	if err != nil {
		log.Fatalf("failed to create etcd client: %v", err)
	}

	// 启动RPC服务器
	log.Printf("rpc server: listening on %s", l.Addr())
	server := distributecache.NewServer(gee, id, "tcp@"+rpcAddr)

	// 将服务器注册到 ETCD
	go distributecache.RegisterServer(etcdClient, *server, leaseTTL)

	server.Accept(l)
	wg.Done()
}

func handleUser(key string, value string, operation string, wg *sync.WaitGroup) (string, string) {
	defer wg.Done()
	fmt.Println("handleUser", key)
	// 构造请求的 URL
	apiURL := fmt.Sprintf("http://0.0.0.0:9999/_geerpc_/users?key=%s", url.QueryEscape(key))

	// 创建一个 HTTP GET 请求
	resp, err := http.Get(apiURL)
	if err != nil {
		log.Printf("failed to send GET request: %v", err)
	}
	defer resp.Body.Close()

	// 读取响应体
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("failed to read response body: %v", err)
	}
	// 检查响应状态码
	if resp.StatusCode != http.StatusOK {
		errMsg := fmt.Sprintf("unexpected status code: %d", resp.StatusCode)
		log.Println(errMsg)
	}
	rpcAddr := strings.TrimSpace(string(body))
	if rpcAddr == "" {
		log.Printf("no RPC address found for key %s", key)
	}
	log.Println(rpcAddr)

	opt := &distributecache.Option{
		MagicNumber:    distributecache.MagicNumber,
		CodecType:      codec.GobType,
		ConnectTimeout: 10 * time.Second,
	}

	client, err := distributecache.XDial(rpcAddr, opt)
	if err != nil {
		log.Fatalf("failed to create new client: %v", err)
	}
	defer client.Close()
	time.Sleep(time.Second)

	var reply string
	switch operation {
	case "Insert":
		args := [2]string{key, value}
		client.Call(context.Background(), "Group.Insert", args, &reply)
		return key, reply
	case "Delete":
		client.Call(context.Background(), "Group.Delete", &key, &reply)
		return key, reply
	case "Search":
		client.Call(context.Background(), "Group.Get", &key, &reply)
		return key, reply
	}
	return "", ""
}
func main() {
	var api bool
	// 解析命令行参数
	mode := flag.String("mode", "", "Mode of operation: 'server' or 'client'")
	rpcAddr := flag.String("addr", "localhost:1234", "Address to listen on or connect to")
	operation := flag.String("operation", "insert", "Insert or Delete or Search")
	key := flag.String("key", "", "key")
	value := flag.String("value", "", "value")
	flag.BoolVar(&api, "api", false, "Start a api server?")
	flag.Parse()

	var wg sync.WaitGroup

	if api {
		wg.Add(1)
		go startAPIServer(&wg)
		wg.Wait()
	}

	wg.Add(1)
	switch *mode {
	case "server":
		startserver(*rpcAddr, &wg)
	case "client":
		k, value := handleUser(*key, *value, *operation, &wg)
		println(k, " ", value)
	default:
		fmt.Println("Invalid mode. Use 'server' or 'client'.")
		os.Exit(1)
	}
	wg.Wait()
}
