package distributecache

import (
	"DistributeCache/singleflight"
	"fmt"
	"log"
	"sync"
)

// 定义接口 Getter 和 回调函数 Get(key string)([]byte, error)，参数是 key，返回值是 []byte。
// 定义函数类型 GetterFunc，并实现 Getter 接口的 Get 方法。
// 函数类型实现某一个接口，称之为接口型函数，方便使用者在调用时既能够传入函数作为参数，也能够传入实现了该接口的结构体作为参数。
// 定义一个函数类型 F，并且实现接口 A 的方法，然后在这个方法中调用自己。这是 Go 语言中将其他函数（参数返回值定义与 F 一致）转换为接口 A 的常用技巧。
type Getter interface {
	Get(key string) ([]byte, error)
}
type GetterFunc func(key string) ([]byte, error)

func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

// 一个 Group 可以认为是一个缓存的命名空间，每个 Group 拥有一个唯一的名称 name。
// 比如可以创建三个 Group，缓存学生的成绩命名为 scores，缓存学生信息的命名为 info，缓存学生课程的命名为 courses。
// 第二个属性是 getter Getter，即缓存未命中时获取源数据的回调(callback)。
// 第三个属性是 mainCache cache，即一开始实现的并发缓存。
// 构建函数 NewGroup 用来实例化 Group，并且将 group 存储在全局变量 groups 中。
// GetGroup 用来特定名称的 Group，这里使用了只读锁 RLock()，因为不涉及任何冲突变量的写操作。
type Group struct {
	name      string
	getter    Getter
	mainCache cache
	loader    *singleflight.Group // 避免缓存击穿
}

var (
	// groups 是一个全局变量，存储了所有的 Group 实例。
	// 使用 sync.RWMutex 保证并发安全。
	mu     sync.RWMutex
	groups = make(map[string]*Group)
)

// NewGroup create a new instance of Group
func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	if getter == nil {
		panic("nil Getter")
	}

	mu.Lock()
	defer mu.Unlock()

	g := &Group{
		name:      name,
		getter:    getter,
		mainCache: cache{cacheBytes: cacheBytes},
		loader:    &singleflight.Group{},
	}

	groups[name] = g
	return g
}

func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

// Get 从 mainCache 中查找缓存
// 如果存在则返回缓存值。
// 缓存不存在，则调用 load 方法，load 调用 getLocally（分布式场景下会调用 getFromPeer 从其他节点获取），getLocally 调用用户回调函数 g.getter.Get() 获取源数据，
// 并且将源数据添加到缓存 mainCache 中（通过 populateCache 方法）
// 参数:
//
//	key - 需要获取值的键。
//
// 返回值:
//
//	ByteView - 键对应的值。
//	error - 如果获取过程中出现错误，则返回错误；否则返回nil。
func (g *Group) Get(key string) (ByteView, error) {
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}
	if v, ok := g.mainCache.get(key); ok {
		log.Println("[GeeCache] hit")
		return v, nil
	}

	return g.load(key)
}

func (g *Group) load(key string) (value ByteView, err error) {
	view, err := g.loader.Do(key, func() (interface{}, error) {
		return g.getLocally(key)
	})

	if err == nil {
		return view.(ByteView), nil
	}
	return
}

func (g *Group) getLocally(key string) (ByteView, error) {
	bytes, err := g.getter.Get(key)
	if err != nil {
		return ByteView{}, err
	}

	value := ByteView{b: cloneBytes(bytes)}
	g.populateCache(key, value)
	return value, err
}

func (g *Group) populateCache(key string, value ByteView) {
	g.mainCache.add(key, value)
}

func (g *Group) Insert(key string, value ByteView) {
	if key == "" {
		log.Println("key is required")
	}
	g.populateCache(key, value)

}

func (g *Group) Delete(key string) error {
	if key == "" {
		log.Println("key is required")
	}
	return g.mainCache.delete(key)
}
