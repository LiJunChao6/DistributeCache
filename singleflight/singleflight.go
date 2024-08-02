package singleflight

import (
	"sync"
)

/// 防止缓存击穿

// call 代表正在进行中，或已经结束的请求。使用 sync.WaitGroup 锁避免重入。
type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

// Group 是 singleflight 的主数据结构，管理不同 key 的请求(call)。
type Group struct {
	mu sync.Mutex
	m  map[string]*call
}

// 在一瞬间有大量请求get(key)，而且key未被缓存或者未被缓存在当前节点
// 如果不用singleflight，那么这些请求都会发送远端节点或者从本地数据库读取，会造成远端节点或本地数据库压力猛增。
// 使用singleflight，第一个get(key)请求到来时，singleflight会记录当前key正在被处理，
// 后续的请求只需要等待第一个请求处理完成，取返回值即可
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	// 获取锁
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	// 当前有相同的请求，则等待那个请求完成直接取其结果
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}

	c := new(call)
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	// 调用 fn，发起请求
	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()

	return c.val, c.err
}
