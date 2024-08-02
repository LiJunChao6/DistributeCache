package distributecache

import (
	"DistributeCache/codec"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"sync"
	"time"
)

const MagicNumber = 0x3bef5c

type Option struct {
	MagicNumber    int
	CodecType      codec.Type
	ConnectTimeout time.Duration
	HandleTimeout  time.Duration
}

var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	CodecType:      codec.GobType,
	ConnectTimeout: time.Second * 10,
}

// 一般来说，涉及协议协商的这部分信息，需要设计固定的字节来传输的。但是为了实现上更简单，GeeRPC 客户端固定采用 JSON 编码 Option，
// 后续的 header 和 body 的编码方式由 Option 中的 CodeType 指定，服务端首先使用 JSON 解码 Option，
// 然后通过 Option 的 CodeType 解码剩余的内容。即报文将以这样的形式发送：

// | Option{MagicNumber: xxx, CodecType: xxx} | Header{ServiceMethod ...} | Body interface{} |
// | <------      固定 JSON 编码      ------>  | <-------   编码方式由 CodeType 决定   ------->|

// 在一次连接中，Option 固定在报文的最开始，Header 和 Body 可以有多个，即报文可能是这样的。
// | Option | Header1 | Body1 | Header2 | Body2 | ...

type Server struct {
	gee  *Group
	ID   string
	Addr string
}

func NewServer(gee *Group, id string, addr string) *Server {
	return &Server{
		gee:  gee,
		ID:   id,
		Addr: addr,
	}
}

// DefaultServer 是一个默认的 Server 实例，主要为了用户使用方便。
// var DefaultServer = NewServer()

// 实现了 Accept 方式，net.Listener 作为参数，for 循环等待 socket 连接建立，并开启子协程处理，处理过程交给了 ServerConn 方法。
func (server *Server) Accept(lis net.Listener) {
	log.Println("rpc server: accept addr:", lis.Addr().String())
	for {
		// net.Listener.Accept() 是Go语言标准库中的一个方法，用于阻塞等待并接受新的网络连接。
		// 当一个新的连接请求到达时，Accept() 方法会返回一个 net.Conn 接口类型的连接实例和一个可能的错误。
		conn, err := lis.Accept()
		if err != nil {
			log.Println("rpc server: accept error:", err)
			return
		}
		go server.ServeConn(conn)
	}
}

const (
	connected        = "200 Connected to Gee RPC"
	defaultRPCPath   = "/_geeprc_"
	defaultDebugPath = "/debug/geerpc"
)

func (server *Server) ServerHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	_, _ = io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	server.ServeConn(conn)
}

// 首先使用 json.NewDecoder 反序列化得到 Option 实例，检查 MagicNumber 和 CodeType 的值是否正确。
// 然后根据 CodeType 得到对应的消息编解码器，接下来的处理交给 serverCodec。
func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	defer func() {
		_ = conn.Close()
	}()
	var opt Option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error: ", err)
		return
	}
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}

	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}
	server.ServeCodec(f(conn), &opt)
}

// 当出错时作为响应函数的参数，表示请求不合法。
var invalidRequest = struct{}{}

func (server *Server) ServeCodec(cc codec.Codec, opt *Option) {
	sending := new(sync.Mutex)
	wg := new(sync.WaitGroup)
	for {
		req, err := server.readRequest(cc) // 读取请求
		if err != nil {
			if req == nil {
				break
			}
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending) // 回复请求
			continue
		}
		wg.Add(1)
		go server.handleRequest(cc, req, sending, wg, opt.HandleTimeout) // 处理请求
	}
	wg.Wait()
	_ = cc.Close()
}

type request struct {
	h            *codec.Header
	argv, replyv reflect.Value
	method       string
}

func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}
func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()

	called := make(chan struct{})
	sent := make(chan struct{})
	go func() {
		var err error
		switch req.method {
		case "Group.Get":
			key := *req.argv.Interface().(*string)
			value, err := server.gee.Get(key)
			if err == nil {
				req.replyv.Elem().Set(reflect.ValueOf(string(value.ByteSlice())))
			}
		case "Group.Insert":
			kv := *req.argv.Interface().(*[2]string)
			value := ByteView{b: []byte(kv[1])}
			server.gee.Insert(kv[0], value)
			*req.replyv.Interface().(*string) = "Insert successful"
		case "Group.Delete":
			key := *req.argv.Interface().(*string)
			err = server.gee.Delete(key)
			if err == nil {
				*req.replyv.Interface().(*string) = "Delete successful"
			} else {
				*req.replyv.Interface().(*string) = "Delete failed"
			}
		}
		called <- struct{}{}
		if err != nil {
			log.Println("rpc server: operator error ", err)
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
		sent <- struct{}{}
	}()
	if timeout == 0 {
		<-called
		<-sent
		return
	}

	select {
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout, expect within %s", timeout)
		server.sendResponse(cc, req.h, invalidRequest, sending)
	case <-called:
		<-sent
	}
}
func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{
		h:      h,
		method: h.ServiceMethod,
	}

	switch req.method {
	case "Group.Get":
		req.argv = reflect.ValueOf(new(string))   // 创建 *string 类型的指针
		req.replyv = reflect.ValueOf(new(string)) // 创建 *string 类型的指针
	case "Group.Insert":
		req.argv = reflect.ValueOf(new([2]string)) // 创建 *[2]string 类型的指针
		req.replyv = reflect.ValueOf(new(string))  // 创建 *bool 类型的指针
	case "Group.Delete":
		req.argv = reflect.ValueOf(new(string))   // 创建 *string 类型的指针
		req.replyv = reflect.ValueOf(new(string)) // 创建 *bool 类型的指针
	default:
		return nil, errors.New("rpc server: unknown method " + req.method)
	}

	argvi := req.argv.Interface()
	if err := cc.ReadBody(argvi); err != nil {
		log.Println("rpc server: read argv err:", err)
		return req, err
	}
	return req, nil
}

func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error: ", err)
	}
}

func NotifyShutdown(registry, addr string) error {
	log.Println("rpc registry: notify shutdown server", addr, "to registry ", registry)
	httpClient := &http.Client{}
	req, _ := http.NewRequest("DELETE", registry, nil)
	req.Header.Set("X-Geerpc-Server", addr)
	if _, err := httpClient.Do(req); err != nil {
		log.Println("rpc server: notify shutdown err:", err)
		return err
	}
	return nil
}
