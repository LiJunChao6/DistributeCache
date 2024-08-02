package distributecache

import (
	consistenthash "DistributeCache/consistentHash"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

const (
	defaultBasePath = "/_geecache/"
	defaultReplice  = 50
)

// 创建具体的 HTTP 客户端类 httpGetter，实现 PeerGetter 接口。
// baseURL 表示将要访问的远程节点的地址，例如 http://example.com/_geecache/。
// 使用 http.Get() 方式获取返回值，并转换为 []bytes 类型。
type httpGetter struct {
	baseURL string
}

// Get 通过HTTP GET请求从指定的URL获取资源。
// group和key用于构建请求的URL路径。
// 返回获取到的字节数据和可能发生的错误。
func (h *httpGetter) Get(group string, key string) ([]byte, error) {
	u := fmt.Sprintf(
		"%v%v/%v",
		h.baseURL,
		url.QueryEscape(group),
		url.QueryEscape(key),
	)
	res, err := http.Get(u)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned: %v", res.Status)
	}

	bytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %v", err)
	}

	return bytes, nil
}
func (h *httpGetter) Insert(group string, key string, value []byte) {
	panic("(h *httpGetter) Insert todo")
}
func (h *httpGetter) Delete(group string, key string) error {
	panic("(h *httpGetter) Delete todo")
}

var _ PeerGetter = (*httpGetter)(nil)

// self，用来记录自己的地址，包括主机名/IP 和端口。
// basePath，作为节点间通讯地址的前缀，默认是 /_geecache/，
// 那么 http://example.com/_geecache/ 开头的请求，就用于节点间的访问。因为一个主机上还可能承载其他的服务，
// 加一段 Path 是一个好习惯。比如，大部分网站的 API 接口，一般以 /api 作为前缀。
type HTTPPool struct {
	self        string
	basePath    string
	mu          sync.Mutex
	peers       *consistenthash.Map    // 用来根据具体的 key 选择节点
	httpGetters map[string]*httpGetter // 映射远程节点与对应的 httpGetter。每一个远程节点对应一个 httpGetter，因为 httpGetter 与远程节点的地址 baseURL 有关。
}

func NewHTTPPool(self string) *HTTPPool {
	return &HTTPPool{
		self:     self,
		basePath: defaultBasePath,
	}
}

// Log info with server name
func (p *HTTPPool) Log(format string, v ...interface{}) {
	log.Printf("[Server %s] %s", p.self, fmt.Sprintf(format, v...))
}

// ServerHTTP 处理所有的HTTP请求。
// 它首先检查请求的URL路径是否以basePath开头，如果不是，则抛出异常。
// 接着，它解析URL路径，提取出group名称和key。
// 然后，它尝试获取对应的group和view，如果不存在，则返回相应的错误。
// 最后，它设置响应的Content-Type并写入view的数据。
func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, p.basePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}
	p.Log("%s, %s", r.Method, r.URL.Path)

	parts := strings.SplitN(r.URL.Path[len(p.basePath):], "/", 2)

	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	groupName := parts[0]
	key := parts[1]

	group := GetGroup(groupName)

	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}

	view, err := group.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(view.ByteSlice())
}

// Set() 方法实例化了一致性哈希算法，并且添加了传入的节点。
// 并为每一个节点创建了一个 HTTP 客户端 httpGetter。
func (p *HTTPPool) Set(peer string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.peers = consistenthash.New(defaultReplice, nil)
	p.peers.Add(peer)
	p.httpGetters = make(map[string]*httpGetter, len(peer))

	p.httpGetters[peer] = &httpGetter{
		baseURL: peer + p.basePath,
	}

}

// PickerPeer() 包装了一致性哈希算法的 Get() 方法，根据具体的 key，选择节点，返回节点对应的 HTTP 客户端。
func (p *HTTPPool) PickPeer(key string) string {
	return ""
	// p.mu.Lock()
	// defer p.mu.Unlock()
	// //根据key找到对应的真实节点
	// if peer := p.peers.Get(key); peer != "" && peer != p.self {
	// 	p.Log("Pick peer %s", peer)
	// 	return p.httpGetters[peer], true
	// }
	// return nil, false
}

var _ PeerPicker = (*HTTPPool)(nil)
