package distributecache

import (
	consistenthash "DistributeCache/consistentHash"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultRPCReplice = 10
	defaultPath       = "/_geerpc_/registry"
	defaultTimeout    = time.Minute * 5
)

type RPCRegistery struct {
	mu      sync.Mutex
	peers   *consistenthash.Map
	timeout time.Duration
	timeMap map[string]*(time.Time)
}

func NewRPCRegistery() *RPCRegistery {
	p := &RPCRegistery{
		timeout: defaultTimeout,
		peers:   consistenthash.New(defaultRPCReplice, nil),
		timeMap: make(map[string]*(time.Time)),
	}
	return p
}

func (p *RPCRegistery) set(peer string) {
	p.peers.Add(peer)
	now := time.Now()
	p.timeMap[peer] = &now
}

func (p *RPCRegistery) PickPeer(key string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	//根据key找到对应的真实节点
	rpcAddr := p.peers.Get(key)
	if rpcAddr != "" {
		if p.timeMap[rpcAddr].Add(p.timeout).Before(time.Now()) {
			log.Printf("peer %s timeout", rpcAddr)
			delete(p.timeMap, rpcAddr)
			p.peers.Remove(rpcAddr)
			return ""
		}
	}
	// log.Println("PickPeer rpcAddr", rpcAddr)
	// if rpcAddr != "" && rpcAddr != p.self {
	// 	if p.rpcgetters[rpcAddr].start.Add(p.timeout).Before(time.Now()) {
	// 		log.Printf("peer %s timeout", rpcAddr)
	// 		delete(p.rpcgetters, rpcAddr)
	// 		p.peers.Remove(rpcAddr)
	// 		return nil, false
	// 	}
	// 	log.Printf("Pick rpcAddr %s", rpcAddr)
	// 	client, err := XDial(rpcAddr, p.opt)
	// 	if err != nil {
	// 		log.Printf("rpc dial %s error %v", rpcAddr, err)
	// 		return nil, false
	// 	}
	// 	p.rpcgetters[rpcAddr].client = client
	// 	return p.rpcgetters[rpcAddr], true
	// }
	return rpcAddr
}

var _ PeerPicker = (*RPCRegistery)(nil)

// putServer：添加服务实例，如果服务已经存在，则更新 start。
func (p *RPCRegistery) putServer(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.timeMap[addr]
	if s == nil {
		p.set(addr)
	} else {
		*s = time.Now()
	}
}

// aliveServers：返回可用的服务列表，如果存在超时的服务，则删除。
func (p *RPCRegistery) aliveServers() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var alive []string
	for addr, s := range p.timeMap {
		if p.timeout == 0 || s.Add(p.timeout).After(time.Now()) {
			alive = append(alive, addr)
		} else {
			delete(p.timeMap, addr)
			p.peers.Remove(addr)
		}
	}
	sort.Strings(alive)
	return alive
}

// Get：返回所有可用的服务列表，通过自定义字段 X-Geerpc-Servers 承载。
// Post：添加服务实例或发送心跳，通过自定义字段 X-Geerpc-Server 承载。
func (p *RPCRegistery) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		w.Header().Set("X-Geerpc-Servers", strings.Join(p.aliveServers(), ","))
	case "POST":
		addr := req.Header.Get("X-Geerpc-Server")
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		log.Println("rpc registry: ServeHTTP putServer ", addr)
		p.putServer(addr)
		log.Println("rpc registry: ServeHTTP putServer end", addr)
	case "DELETE":
		addr := req.Header.Get("X-Geerpc-Server")
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		log.Println("rpc registry: ServeHTTP removeServer ", addr)
		delete(p.timeMap, addr)
		p.peers.Remove(addr)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (p *RPCRegistery) HandleHTTP(registryPath string) {
	http.Handle(registryPath, p)
	log.Printf("rpc registry path: %s", registryPath)
}

// 服务启动时定时向注册中心发送心跳，默认周期比注册中心设置的过期时间少 1 min。
func Heartbeat(registry, addr string, duration time.Duration) {
	if duration == 0 {
		duration = defaultTimeout - time.Duration(1)*time.Minute
		log.Println("rpc registry: default duration is ", duration)
	}
	var err error
	err = sendHeartbeat(registry, addr)
	go func() {
		t := time.NewTicker(duration)
		for err == nil {
			<-t.C
			err = sendHeartbeat(registry, addr)
		}
	}()
}

// 定期处理过期节点
func (p *RPCRegistery) Cleanup() {
	go func() {
		t := time.NewTicker(time.Minute)
		for {
			<-t.C
			p.mu.Lock()
			for addr, s := range p.timeMap {
				if s.Add(p.timeout).Before(time.Now()) {
					delete(p.timeMap, addr)
					p.peers.Remove(addr)
					log.Printf("rpc registry: remove expired server %s", addr)
				}
			}
			p.mu.Unlock()
		}
	}()
}

func sendHeartbeat(registry, addr string) error {
	log.Println("rpc registry: heart beat to registry ", registry)
	httpClient := &http.Client{}
	req, _ := http.NewRequest("POST", registry, nil)
	req.Header.Set("X-Geerpc-Server", addr)
	if _, err := httpClient.Do(req); err != nil {
		log.Println("rpc server: heart beat err:", err)
		return err
	}
	return nil
}
