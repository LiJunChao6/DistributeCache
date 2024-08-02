package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// 定义了函数类型 Hash，
// 采取依赖注入的方式，允许用于替换成自定义的 Hash 函数，也方便测试时替换，默认为 crc32.ChecksumIEEE 算法。
// Map 是一致性哈希算法的主数据结构，包含 4 个成员变量：
// Hash 函数 hash；
// 虚拟节点倍数 replicas；
// 哈希环 keys；
// 虚拟节点与真实节点的映射表 hashMap，键是虚拟节点的哈希值，值是真实节点的名称。
type Hash func(data []byte) uint32

type Map struct {
	hash     Hash
	replicas int
	keys     []int
	hashMap  map[int]string
}

// 构造函数 New() 允许自定义虚拟节点倍数和 Hash 函数
func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}

	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}

	return m
}

// Add 方法用于向 Map 中添加键。
// 对每一个真实节点 key，对应创建 m.replicas 个虚拟节点，虚拟节点的名称是：strconv.Itoa(i) + key，即通过添加编号的方式区分不同虚拟节点。
// 使用 m.hash() 计算虚拟节点的哈希值，使用 append(m.keys, hash) 添加到环上。
// 在 hashMap 中增加虚拟节点和真实节点的映射关系。
// 最后一步，环上的哈希值排序。
func (m *Map) Add(key string) {
	for i := 0; i < m.replicas; i++ {
		hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
		m.keys = append(m.keys, hash)
		m.hashMap[hash] = key
	}
	sort.Ints(m.keys)
}

// 实现选择节点的 Get() 方法。
// 第一步，计算 key 的哈希值。
// 第二步，顺时针找到第一个匹配的虚拟节点的下标 idx，从 m.keys 中获取到对应的哈希值。如果 idx == len(m.keys)，说明应选择 m.keys[0]，因为 m.keys 是一个环状结构，所以用取余数的方式来处理这种情况。
// 第三步，通过 hashMap 映射得到真实的节点。
func (m *Map) Get(key string) string {
	if len(m.keys) == 0 {
		return ""
	}

	hash := int(m.hash([]byte(key)))
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	return m.hashMap[m.keys[idx%len(m.keys)]]
}

// Remove 方法用于从一致性哈希 Map 中删除一个真实节点及其所有虚拟节点。
// 首先遍历 m.replicas 次，每次计算虚拟节点的哈希值。
// 使用 sort.SearchInts 在 m.keys 中查找哈希值的位置。
// 如果找到了对应的哈希值，则从 m.keys 切片中移除，并且从 hashMap 中删除相应的键值对。
// 这个过程重复 m.replicas 次，以确保所有虚拟节点都被删除。
func (m *Map) Remove(key string) {
	for i := 0; i < m.replicas; i++ {
		hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
		idx := sort.SearchInts(m.keys, hash)
		if idx < len(m.keys) && m.keys[idx] == hash {
			delete(m.hashMap, hash)
			m.keys = append(m.keys[:idx], m.keys[idx+1:]...)
		}
	}
}
